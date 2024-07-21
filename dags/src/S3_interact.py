import io
import os
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.fp_log_config import logger


class S3BucketAccess:
    def __init__(self, bucket_name, folder_name):
        try:
            self.AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
            self.AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
            self.AWS_SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
        except KeyError as aws_credentials_error:
            logger.error(f'AWS credentials error. Not set: {aws_credentials_error.args}.')
            raise aws_credentials_error
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.client = boto3.client(
            service_name='s3',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            aws_session_token=self.AWS_SESSION_TOKEN,
        )
        if not self.client:
            logger.error('Cannot connect to S3 service.')
        else:
            logger.info('Connected to S3.')

    def get_objects_list(self):
        return self.client.list_objects_v2(Bucket=self.bucket_name,
                                           Prefix=self.folder_name,
                                           Delimiter='\''
                                           )

    def save_df_to_S3_csv(self,
                          object_name: str,
                          df: pd.DataFrame,
                          mode='w',
                          index=False,
                          header=True,
                          ):
        buffer_io = StringIO()
        df.to_csv(buffer_io, header=header, index=index, mode=mode)
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=self.folder_name + '/' + object_name,
            Body=buffer_io.getvalue()
        )
        return f'{self.client.meta.endpoint_url}/{self.bucket_name}/{self.folder_name}/{object_name}'

    def save_df_to_S3_parquet(self,
                          object_name: str,
                          df: pd.DataFrame,
                          index=False,
                          ):
        # buffer_io = StringIO()
        df.to_parquet(object_name, index=index)
        # self.client.put_object(
        #     Bucket=self.bucket_name,
        #     Key=self.folder_name + '/' + object_name,
        #     Body=buffer_io.getvalue()
        # )
        self.upload_file(object_name)
        os.remove(object_name)
        return f's3://{self.bucket_name}/{self.folder_name}/{object_name}'


    def upload_file(self,
                    filename: str):
        with open(filename, 'rb') as file_to_upload:
            self.client.upload_file(filename, self.bucket_name, self.folder_name + '/' + filename)

    def load_object_from_S3(self, file_name: str, column_names: list) -> pd.DataFrame:
        '''loads CSV files from S3'''
        path = f's3://{self.bucket_name}/{self.folder_name}/{file_name}'
        try:
            df = pd.read_csv(path, sep=',',
                             names=column_names,
                             on_bad_lines='warn',
                             encoding='utf-8',
                             skiprows=1,
                             )
        except UnicodeDecodeError as e:
            df = pd.read_csv(path,
                             sep=',',
                             names=column_names,
                             on_bad_lines='warn',
                             encoding='cp1252',
                             skiprows=1,
                             )
        return df

    def load_parquet_from_S3(self, object_name: str) -> pd.DataFrame:
        '''loads parquet file from S3'''
        try:
            object = self.client.get_object(
                Bucket=self.bucket_name,
                Key=self.folder_name + '/' + object_name,
            )
            df = pd.read_parquet(io.BytesIO(object['Body'].read()))
            return df
        except ClientError as e:
            logger.error(f'Error reading {object_name} from S3.')
            return None


def return_S3_access_object() -> S3BucketAccess:
    S3_writer = S3BucketAccess(os.environ['BUCKET_NAME'], os.environ['S3_FOLDER_NAME'])
    return S3_writer
