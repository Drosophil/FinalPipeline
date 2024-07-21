import os
import os

import chembl_downloader
import pandas as pd
import psycopg2
from psycopg2.errors import UniqueViolation
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from src.fp_log_config import logger


# path = chembl_downloader.download_extract_sqlite()
# print(path)

class DataLoaderToRDS():
    def __init__(self):
        try:
            self.DB_HOST = os.environ['DB_HOST']
            self.DB_PORT = int(os.environ['DB_PORT'])
            self.DB_NAME = os.environ['DB_NAME']
            self.DB_USER = os.environ['DB_USER']
            self.DB_PASSWORD = os.environ['DB_PASSWORD']
        except KeyError as db_credentials_error:
            logger.error(f'DB credentials error. Not set: {db_credentials_error.args}.')
            raise db_credentials_error

        self.db = psycopg2.connect(
            host=self.DB_HOST,
            port=self.DB_PORT,
            dbname=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
        )
        if not self.db:
            logger.error('Cannot connect to RDS.')
        else:
            logger.info('Connected to DB.')

        url_object = URL.create(
            "postgresql+psycopg2",
            username=self.DB_USER,
            password=self.DB_PASSWORD,  # plain (unescaped) text
            host=self.DB_HOST,
            database=self.DB_NAME,
            port=self.DB_PORT,
        )

        self.alc_conn = create_engine(url_object)
        self.alc_conn.connect()
        self.db.commit()

    def check_and_construct_bronze_tables(self,
                                          tables=['bronze_molecule_dictionary',
                                                  'bronze_compound_properties',
                                                  'bronze_compound_structures',
                                                  'bronze_chembl_id_lookup',
                                                  ],
                                          construct=False):
        '''Checks if tables are in DB: True/False
        If construct=True parameter is set, then downloads Chembl and constructs tables'''
        existing_tables=[]
        for table in tables:
            existing_tables.append(self.check_if_exists(table))
        if False in existing_tables:
            logger.info('Tables are missing.')
            if construct:
                logger.info('Downloading CHEMBL...')
                path = chembl_downloader.download_extract_sqlite()
                logger.info('CHEMBL dowloaded.')
                for if_exists, table_name in zip(existing_tables, tables):
                    if not if_exists:
                        logger.info(f'Fetching table {table_name}.')
                        sql = f'select * from {table_name.replace("bronze_", "")};'
                        df = chembl_downloader.query(sql)
                        logger.info(f'Inserting data from table {table_name}.')
                        self.insert_data_to_RDS(df, table_name, if_exists='replace')
                # os.remove(path)
            else:
                return False
        else:
            logger.info('All tables are already in DB.')
            return True


    def insert_data_to_RDS(self,
                           df: pd.DataFrame,
                           name: str,
                           if_exists='replace',
                           ):
        '''Inserts a DataFrame in the DB as a table'''
        try:
            df.to_sql(name, con=self.alc_conn, if_exists=if_exists, index=False)
        except UniqueViolation as e:
            logger.info(f'Diplicate found. Msg: {e}')


    def check_if_exists(self,
                        name: str,
                        ):
        '''Checks if a table exists in DB'''
        check_result = False
        try:
            cur = self.db.cursor()
            cur.execute("select exists(select relname from pg_class where relname='" + name + "')")
            check_result = cur.fetchone()[0]
            cur.close()
        except psycopg2.Error as e:
            pass
        finally:
            print(check_result)
            return check_result

    def query_executor(self,
                       query: str) -> pd.DataFrame:
        '''Query executor for SELECT DB operations'''
        try:
            cur = self.db.cursor()
            cur.execute(query)
            colnames = [desc[0] for desc in cur.description]
            result = cur.fetchall()
            if result:
                return pd.DataFrame(result, columns=colnames)
            else:
                cur.close()
                return None
        except psycopg2.Error as e:
            logger.error(f'Error executing query {query}: {e}')
            raise e

    def insert_query_executor(self,
                              query: str,
                              value: str,
                              ):
        '''Query executor for DB INSERT operations'''
        try:
            cur = self.db.cursor()
            result = cur.execute(query, (value,))
            self.db.commit()
            cur.close()
            if result:
                return result
            else:
                return None
        except psycopg2.Error as e:
            logger.error(f'Insertion to table error: {e}')
            raise e

    def populate_dim_table_query_executor(self,
                              query: str,
                              ):
        '''Query executor for DB INSERT operations'''
        try:
            cur = self.db.cursor()
            result = cur.execute(query)
            self.db.commit()
            cur.close()
            if result:
                return result
            else:
                return None
        except UniqueViolation as e:
            logger.warning(f'Unique violation: {e}')
            self.db.rollback()


    def create_query_executor(self,
                       query: str):
        '''Query executor for DB CREATE operations'''
        try:
            cur = self.db.cursor()
            cur.execute(query)
            self.db.commit()
            cur.close()
        except UniqueViolation as e:
            logger.error(f'Error creating tables: {e}')
            raise e


    def db_paginator(self,
                     query: str,
                     page: int,
                     limit: int):
        '''Method for pagination, unfortunately does not work'''
        result = None
        offset = 0
        while (offset <= limit) and (page < limit):
            try:
                cur = self.db.cursor()
                cur.execute(query + f" limit {page} offset {offset};")
                result = cur.fetchall()
                if result:
                    offset += page
                else:
                    cur.close()
            except psycopg2.Error as e:
                yield StopIteration
            finally:
                yield result

def return_db_object() -> DataLoaderToRDS:
    data_load = DataLoaderToRDS()
    return data_load

# sql = """
# select * from compound_properties;
# """
# df = chembl_downloader.query(sql)
# data_load.insert_data_to_RDS(df, 'bronze_compound_properties')
# print('table 2 done')
# sql = """
# select * from compound_structures;
# """
# df = chembl_downloader.query(sql)
# data_load.insert_data_to_RDS(df, 'bronze_compound_structures')
# print('table 3 done')
