import pandas as pd

from src.S3_interact import S3BucketAccess
from src.db_interact import DataLoaderToRDS
from src.mpp import MolecularPropertiesProcessor


def compute_source_morgan_fingerprints(data_load: DataLoaderToRDS, S3_writer: S3BucketAccess):
    '''Starts parallel computing of chembl morgan fingerprints'''

    query = "select count(*) from bronze_compound_structures;"
    df = data_load.query_executor(query)
    number_of_rows = int(df.iloc[0,0])
    number_of_iterations = 5  # empiric number of chunks
    pagination = number_of_rows // number_of_iterations
    lower_limit = 0
    header = True
    mode='w'
    for iter_count in range(number_of_iterations):
        query = f'select molregno, canonical_smiles' \
                f' from bronze_compound_structures' \
                f' where ((molregno >= {lower_limit}) and (molregno < {lower_limit + pagination}));'
        df = data_load.query_executor(query)  # set the dataframe
        # executor instance
        mpp_instance = MolecularPropertiesProcessor(
            df,
            target_col='canonical_smiles',
            mol_name_col='molregno',
            hyperthreading=True  # we need real physical cores for the processes
        )
        final_result = mpp_instance.process_data()
        if iter_count > 0:
            header = False
            mode='a'
        final_result.to_csv('fingerprints.csv',
                                    header=header,
                                    mode=mode,
                                    index=False,
                            )
        # S3_writer.save_df_to_S3_csv(f'fingerprints_{iter_count}.csv',
        #                             final_result,
        #                             header=header,
        #                             mode='w')
        lower_limit += pagination
        print(final_result)
    S3_writer.upload_file('fingerprints.csv')

def compute_target_morgan_fingerprints(df: pd.DataFrame) -> pd.DataFrame:
    '''Starts parallel computing of targets morgan fingerprints'''

    number_of_rows = len(df.index)
    if number_of_rows > 0:
        # executor instance
        mpp_instance = MolecularPropertiesProcessor(
            df,
            target_col='smiles',
            mol_name_col='chembl_id',
            hyperthreading=True  # we need real physical cores for the processes
        )
        final_result = mpp_instance.process_data()
        return final_result
    else:
        return None
