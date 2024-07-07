# 1. Read and transform target files
# 2. Read and transform source fingerprints
# 3. Compute Tanimoto similarity score
# 4. Save Tanimoto scores as parquet

import os

from db_interact import data_load
from S3_interact import S3BucketAccess, S3_writer

def get_input_files_list() -> list:
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
        input_files_list_str = ' '
        input_files_list_str = input_files_list_str.join(input_files_list)
        query = 'insert into saved_used_input_files (file_list) VALUES (%s);'
        data_load.insert_query_executor(query, input_files_list_str)
    return input_files_list


get_input_files_list()