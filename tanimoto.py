# 1. Read and transform target files
# 2. Read and transform source fingerprints
# 3. Compute Tanimoto similarity score
# 4. Save Tanimoto scores as parquet

import os
import re

import pandas as pd
import numpy as np
from rdkit import Chem, DataStructs
from rdkit.DataStructs.cDataStructs import ExplicitBitVect, TanimotoSimilarity

from db_interact import DataLoaderToRDS, return_db_object
from S3_interact import S3BucketAccess, return_S3_access_object
from compute_morgan import compute_target_morgan_fingerprints


def get_input_files_list(data_load: DataLoaderToRDS) -> list:
    '''reads a list of .csv files in an input folder
    and removes from it those files that are already in DB'''

    input_files_list = []
    S3_input_files = S3BucketAccess(os.environ['BUCKET_NAME'], 'final_task/input_files')
    for key in S3_input_files.get_objects_list()['Contents']:
        filepath = key['Key'].replace('final_task/input_files/', '')
        if '.csv' in filepath:
            input_files_list.append(filepath)

    if data_load.check_if_exists('saved_used_input_files'):
        query = 'select file_list from saved_used_input_files;'
        used_files_list = data_load.query_executor(query)
        if used_files_list is not None:
            used_files_list = used_files_list.iloc[0, 0].split()
        else:
            used_files_list = []
        list_to_remove = []
        for each_file in input_files_list:
            if each_file in used_files_list:
                list_to_remove.append(each_file)
        for each_file in list_to_remove:
            input_files_list.remove(each_file)
        used_files_list += input_files_list
        used_files_list_str = ' '.join(used_files_list)
        query = 'truncate table saved_used_input_files;'
        data_load.create_query_executor(query)
        query = 'insert into saved_used_input_files (file_list) VALUES (%s);'
        data_load.insert_query_executor(query, used_files_list_str)
    else:
        query = 'create table if not exists saved_used_input_files (file_list VARCHAR);'
        data_load.create_query_executor(query)
        input_files_list_str = ' '.join(input_files_list)
        query = 'insert into saved_used_input_files (file_list) VALUES (%s);'
        data_load.insert_query_executor(query, input_files_list_str)
        #   -------------------------------------------------------------------------------
        # TODO: remove these 2 lines
        query = 'drop table saved_used_input_files;'
        data_load.create_query_executor(query)
        #   -------------------------------------------------------------------------------
    input_files_list.sort(reverse=True)
    return input_files_list

def clean_the_data(df: pd.DataFrame) -> pd.DataFrame:
    '''Repair damaged CHEMBL_IDs, remove duplicates, extras'''
    def set_chembl_id(s: str) -> str:
        lst = re.findall(r'\d+', s)
        return 'CHEMBL' + lst[0]
    df['chembl_id'] = df['molecule name'].apply(lambda s: set_chembl_id(s))
    df = df.drop(['molecule name', Ellipsis], axis=1)
    column_titles = ['chembl_id', 'smiles']
    df = df.reindex(columns=column_titles)
    df.drop_duplicates(subset='chembl_id', inplace=True)
    return df

def read_input_files(data_load: DataLoaderToRDS):
    '''Read input files do DataFrames'''
    input_files = get_input_files_list(data_load=data_load)
    if input_files:
        S3_input_files = S3BucketAccess(os.environ['BUCKET_NAME'], 'final_task/input_files')
        results = []
        column_names = ['molecule name', 'smiles', ...]
        for file in input_files:
            df = S3_input_files.load_object_from_S3(file_name=file, column_names=column_names)
            if len(df.index) > 0:
                df = clean_the_data(df)
                results.append(compute_target_morgan_fingerprints(df))
        return results
    else:
        return None

def extract_bit_vector(s):
    '''extract bit vector fingerprint from a hex string'''
    fingerprint = ExplicitBitVect(2048)
    fingerprint.FromBase64(s)
    return fingerprint

def load_chembl_fingerprints(S3_writer: S3BucketAccess) -> pd.DataFrame:
    '''load saved CHEMBL fingerprints from S3'''
    file = "fingerprints.csv"
    column_names = ['molregno', 'morgan_fingerprint']
    source_mols = S3_writer.load_object_from_S3(file_name=file, column_names=column_names)
    # extract morgan bit vectors back:
    source_mols['morgan_bit_vector'] = source_mols['morgan_fingerprint'].apply(lambda s: extract_bit_vector(s))
    source_mols = source_mols.drop('morgan_fingerprint', axis=1)
    return source_mols

def get_output_bucket_file_list(S3_writer: S3BucketAccess) -> list:
    '''get list of files from output bucket'''
    file_list = []
    for key in S3_writer.get_objects_list()['Contents']:
        filepath = key['Key'].replace(os.environ['S3_FOLDER_NAME'] + '/', '')
        # if '.parquet' in filepath:
        file_list.append(filepath)
        # file_list checked to be OK!
    return file_list

def compute_tanimoto_similarities(
        target_mols: pd.DataFrame,
        source_mols: pd.DataFrame,
        input_files: list)\
        :
    '''calculate Tanimoto similarity scores'''
    result = []
    for target_row in target_mols.iterrows():
        file_name = 'similarity_' + target_row[1]['chembl_id'] + '.parquet'
        source_mols_copy = source_mols.copy()
        if file_name in input_files:
            break  # if this molecule is already matched, hat no need to do it again
        source_mols_copy['target_molregno'] = target_row[1]['entity_id']
        source_mols_copy['target_chembl_id'] = target_row[1]['chembl_id']
        source_mols_copy['similarity'] = source_mols_copy['morgan_bit_vector']\
            .apply(lambda v: TanimotoSimilarity(target_row[1]['morgan_bit_vector'], v))
        source_mols_copy = source_mols_copy.drop('morgan_bit_vector', axis=1)
        indexMols = source_mols_copy[
            source_mols_copy['molregno'] == source_mols_copy['target_molregno']
        ].index
        source_mols_copy.drop(indexMols, inplace=True)
        print(source_mols_copy.sort_values(by=['similarity'], ascending=False).head())  # TODO: <--- Remove this!
        result.append((file_name, source_mols_copy))
    return result


def get_similarities(data_load: DataLoaderToRDS, S3_writer: S3BucketAccess):
    '''get similarity scores for each target and save them in S3'''
    target_mols = read_input_files(data_load=data_load)
    if target_mols:
        source_mols = load_chembl_fingerprints(S3_writer=S3_writer)

        file_list = get_output_bucket_file_list(S3_writer=S3_writer)
        print(file_list)  # TODO: <-- remove this

        #  <<<<<< following is for getting CHEMBL_ID from MOLREGNO,
        #  <<<<<< and in works, but
        #  <<<<<< joining tables in DB is much faster (1 query vs N queries).
        #
        # def get_molregno_from_chembl_id(s: str) -> int:
        #     query = "select entity_id from bronze_chembl_id_lookup where chembl_id='" + s + "';"
        #     result = data_load.query_executor(query)
        #     print(len(result.index), result.iloc[0, 0])
        #     return result.iloc[0, 0]

        for every_set in target_mols:
            #
            # <<<<<< this is for the same purpose (see comment upper).
            #
            # every_set['molregno'] = every_set['chembl_id'].apply(lambda s: get_molregno_from_chembl_id(s))
            # every_set = every_set.reindex(columns=['molregno', 'chembl_id', 'morgan_fingerprint'])
            # print(every_set.head())

            data_load.insert_data_to_RDS(every_set, 'bronze_temporary', if_exists='replace')
            query = '''select b.entity_id, b.chembl_id, a.morgan_fingerprint 
            from bronze_temporary a 
            left join bronze_chembl_id_lookup b 
            on a.chembl_id=b.chembl_id;'''
            every_set = data_load.query_executor(query)
            data_load.create_query_executor('drop table bronze_temporary;')

            # extract morgan bit vectors back:
            every_set['morgan_bit_vector'] = every_set['morgan_fingerprint'].apply(lambda s: extract_bit_vector(s))
            every_set = every_set.drop('morgan_fingerprint', axis=1)
            print(every_set.head())  # TODO: <--- remove this at prod

            #  now we can compute Tanimoto similarities

            set_result = compute_tanimoto_similarities(every_set, source_mols, file_list)
            for each_result in set_result:
                msg = S3_writer.save_df_to_S3_parquet(each_result[0], each_result[1])
                print(msg)
    else:
        return None



if __name__=='__main__':
    data_load = return_db_object()
    S3_writer = return_S3_access_object()
    get_similarities(data_load=data_load, S3_writer=S3_writer)