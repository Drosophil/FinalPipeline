import numpy as np
import pandas as pd

from src.S3_interact import S3BucketAccess, return_S3_access_object
from src.db_interact import DataLoaderToRDS, return_db_object
from src.tanimoto import get_output_bucket_file_list


def load_similarities_from_parquet(S3_writer: S3BucketAccess):
    '''loads all similarities files to a list of pandas dataframes'''
    file_list = get_output_bucket_file_list(S3_writer=S3_writer)
    if file_list:
        dataframes_list = []
        # TODO: remove this 2 lines
        # temp_list = [file_list[i] for i in range(2)]
        # file_list = temp_list

        for each_file in file_list:
            if (('.parquet' in each_file) and ('similarity_CHEMBL' in each_file)):
                df = S3_writer.load_parquet_from_S3(each_file)
                if df is not None:
                    dataframes_list.append(df)
        return dataframes_list
    else:
        return None


def get_top_10_similarities(df: pd.DataFrame):
    '''returnes top 10 chembl similarities dataframe for a given target'''
    if df is not None:
        top_10_df = df.sort_values(by=['similarity'], ascending=False).head(10)
        last_largest_score = top_10_df['similarity'].min()
        last_largest_score_top10 = top_10_df.loc[np.isclose(top_10_df['similarity'], last_largest_score)]
        last_largest_score_overall = df.loc[np.isclose(df['similarity'], last_largest_score)]
        top_10_df['has_duplicates_of_last_largest_score'] = 0
        num_last_largest = len(last_largest_score_overall)
        if (len(last_largest_score_top10) != num_last_largest):
            top_10_df.loc[np.isclose(top_10_df['similarity'], last_largest_score),\
                'has_duplicates_of_last_largest_score'] = num_last_largest
        top_10_df.rename(columns={'molregno': 'source_molregno', 'similarity': 'tanimoto_similarity'}, inplace=True)
        top_10_df.drop('target_chembl_id', axis=1, inplace=True)
        # TODO: remove commented lines
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(top_10_df)
        return top_10_df
    else:
        return None


def construct_facts_dataframe(data_load: DataLoaderToRDS, S3_writer: S3BucketAccess):
    '''constructs facts dataframe'''
    list_of_target_sims = load_similarities_from_parquet(S3_writer=S3_writer)
    if list_of_target_sims:
        facts_df = pd.DataFrame()
        for each_target in list_of_target_sims:
            if each_target is not None:
                top_10 = get_top_10_similarities(each_target)
                facts_df = pd.concat([facts_df, top_10], ignore_index=True)
        query = 'SELECT source_molregno, target_molregno FROM silver_fact;'
        facts_already_in_db = data_load.query_executor(query)
        if (facts_already_in_db is not None) and (len(facts_already_in_db) > 0):
            # Find and drop duplicates that are already in the DB:
            facts_already_in_db_list = []
            facts_to_be_added_list = []
            def get_unique_pairs(a: int, b: int, worklist: list):
                worklist.append((a, b))
            facts_already_in_db.apply(lambda x: get_unique_pairs(x['source_molregno'],
                                                                 x['target_molregno'],
                                                                 facts_already_in_db_list
                                                                 ), axis=1)
            facts_df.apply(lambda x: get_unique_pairs(x['source_molregno'],
                                                                 x['target_molregno'],
                                                                 facts_to_be_added_list
                                                                 ), axis=1)
            for a in facts_to_be_added_list:
                if a in facts_already_in_db_list:
                    index_of_a_duplicate_row = facts_df[((facts_df['source_molregno'].eq(a[0])) &
                                                         (facts_df['target_molregno'].eq(a[1])))].index
                    facts_df.drop(index_of_a_duplicate_row, inplace=True)
            # duplicates dropped. In normal workflow there should not be duplicates, but just in case
        return facts_df
    else:
        return None


def construct_dim_draft_dataframe(data_load: DataLoaderToRDS, facts_df: pd.DataFrame):
    '''constructs a dim addition draft (column with unique mol ID's from a given dataframe)'''
    if facts_df is not None:
        query = 'SELECT molregno FROM silver_dim_molecules;'
        already_there_list = []
        def from_df_to_list(n: int):
            already_there_list.append(n)
        already_there = data_load.query_executor(query)
        if already_there is not None:
            already_there['molregno'].apply(lambda n: from_df_to_list(n))
        mol_id_list = []
        def fill_the_list(n: int):
            if (n not in mol_id_list) and (n not in already_there_list):
                mol_id_list.append(n)
        facts_df['source_molregno'].apply(lambda n: fill_the_list(n))
        facts_df['target_molregno'].apply(lambda n: fill_the_list(n))
        mol_id_df = pd.DataFrame({'molregno': mol_id_list})
        return mol_id_df
    else:
        return None


def silver_tables_check_and_create(data_load: DataLoaderToRDS):
    '''checks if silver tables exist and, if not, creates them'''
    query = '''
        CREATE TABLE IF NOT EXISTS silver_dim_molecules (
        molregno INTEGER PRIMARY KEY,
        chembl_id VARCHAR,
        molecule_type VARCHAR,
        mw_freebase FLOAT,
        alogp FLOAT,
        psa FLOAT,
        cx_logp FLOAT,
        molecular_species VARCHAR,
        full_mwt FLOAT,
        aromatic_rings FLOAT,
        heavy_atoms FLOAT,
        UNIQUE(molregno)
        );
    '''
    data_load.create_query_executor(query)
    query = '''
        CREATE TABLE IF NOT EXISTS silver_fact (
        id SERIAL PRIMARY KEY,
        source_molregno INTEGER,
        target_molregno INTEGER,
        tanimoto_similarity FLOAT,
        has_duplicates_of_last_largest_score INTEGER,
        UNIQUE (source_molregno, target_molregno),
        CONSTRAINT fk_source_mol
            FOREIGN KEY(source_molregno)
                REFERENCES silver_dim_molecules(molregno)
                ON DELETE CASCADE,
        CONSTRAINT fk_target_mol
            FOREIGN KEY(target_molregno)
                REFERENCES silver_dim_molecules(molregno)
                ON DELETE CASCADE
    );
    '''
    data_load.create_query_executor(query)


def injest_silver_tables(data_load: DataLoaderToRDS, S3_writer: S3BucketAccess):
    '''insert to db and drop duplicates'''
    silver_tables_check_and_create(data_load=data_load)
    facts_df = construct_facts_dataframe(data_load=data_load, S3_writer=S3_writer)
    dim_draft = construct_dim_draft_dataframe(data_load=data_load, facts_df=facts_df)
    if dim_draft is not None:
        data_load.insert_data_to_RDS(dim_draft, 'bronze_dim_draft', if_exists='replace')
        query = '''
        INSERT INTO silver_dim_molecules (molregno, chembl_id, molecule_type, 
        mw_freebase, alogp, psa, cx_logp, molecular_species, 
        full_mwt, aromatic_rings, heavy_atoms)
        SELECT a.molregno, b.chembl_id, c.molecule_type, 
        d.mw_freebase, d.alogp, d.psa, d.cx_logp, d.molecular_species, 
        d.full_mwt, d.aromatic_rings, d.heavy_atoms 
        FROM bronze_dim_draft a 
        JOIN bronze_chembl_id_lookup b on a.molregno = b.entity_id 
        JOIN bronze_molecule_dictionary c on a.molregno = c.molregno 
        JOIN bronze_compound_properties d on a.molregno = d.molregno
        WHERE b.entity_type = 'COMPOUND';
        '''
        data_load.populate_dim_table_query_executor(query)
        query = 'drop table bronze_dim_draft;'
        data_load.create_query_executor(query)
    if facts_df is not None:
        data_load.insert_data_to_RDS(facts_df, 'silver_fact', if_exists='append')


if __name__=='__main__':
    data_load = return_db_object()
    S3_writer = return_S3_access_object()
    # entry_point(data_load=data_load, S3_writer=S3_writer)
    injest_silver_tables(data_load=data_load, S3_writer=S3_writer)


