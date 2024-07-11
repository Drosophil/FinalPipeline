import os
import re

import pandas as pd
import numpy as np

from db_interact import DataLoaderToRDS, return_db_object
from S3_interact import S3BucketAccess, return_S3_access_object
from tanimoto import get_output_bucket_file_list


def load_similarities_from_parquet(S3_writer: S3BucketAccess) -> list | None:
    '''loads all similarities files to a list of pandas dataframes'''
    file_list = get_output_bucket_file_list(S3_writer=S3_writer)
    if file_list:
        dataframes_list = []
        # TODO: remove this 2 lines
        # temp_list = [file_list[i] for i in range(4)]
        # file_list = temp_list

        for each_file in file_list:
            if (('.parquet' in each_file) and ('similarity_CHEMBL' in each_file)):
                df = S3_writer.load_parquet_from_S3(each_file)
                if df is not None:
                    dataframes_list.append(df)
        return dataframes_list
    else:
        return None


def get_top_10_similarities(df: pd.DataFrame) -> pd.DataFrame | None:
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


def construct_facts_dataframe(S3_writer: S3BucketAccess) -> pd.DataFrame | None:
    '''constructs facts dataframe'''
    list_of_target_sims = load_similarities_from_parquet(S3_writer=S3_writer)
    if list_of_target_sims:
        facts_df = pd.DataFrame
        for each_target in list_of_target_sims:
            if each_target is not None:
                facts_df._append(get_top_10_similarities(each_target), ignore_index=True)
        return facts_df
    else:
        return None


def construct_dim_draft_dataframe(facts_df: pd.DataFrame) -> pd.DataFrame | None:
    '''constructs a dim addition draft (column with unique mol ID's from a given dataframe)'''
    if facts_df is not None:
        mol_id_list = []
        def fill_the_list(n: int):
            if n not in mol_id_list:
                mol_id_list.append(n)
        facts_df['source_molregno'].apply(lambda n: fill_the_list(n))
        facts_df['target_molregno'].apply(lambda n: fill_the_list(n))
        mol_id_df = pd.DataFrame({'molregno': mol_id_list})
        return mol_id_df
    else:
        return None


def injest_silver_tables(data_load: DataLoaderToRDS, S3_writer: S3BucketAccess) -> bool:
    '''insert to db and drop duplicates'''
    facts_df = construct_facts_dataframe(data_load=data_load, S3_writer=S3_writer)
    dim_draft = construct_dim_draft_dataframe(facts_df=facts_df)
    '''
    here is the future code to put those dataframes to RDS with S3BucketAccess object
    '''

if __name__=='__main__':
    data_load = return_db_object()
    S3_writer = return_S3_access_object()
    # entry_point(data_load=data_load, S3_writer=S3_writer)
    injest_silver_tables(data_load=data_load, S3_writer=S3_writer)


