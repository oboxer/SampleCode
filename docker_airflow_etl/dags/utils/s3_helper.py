import os
import pathlib

import boto3
from utils.db_helper import S3_SETTINGS


def __get_s3_conn() -> boto3.client:
    s3_client = boto3.client('s3',
                             aws_access_key_id=S3_SETTINGS['aws_access_key_id'],
                             aws_secret_access_key=S3_SETTINGS['aws_secret_access_key'],
                             region_name='US-EAST-1'
                             )
    return s3_client


def upload_file_to_s3(bucket_name: str, key: str, file: str):
    s3_client = __get_s3_conn()
    s3_client.upload_file(file, bucket_name, key)


def write_to_file(content: str, dir_name: str, file_name: str):
    if not pathlib.Path(dir_name).exists():
        os.makedirs(dir_name, exist_ok=True)

    file = pathlib.Path(dir_name + file_name)
    if file.exists():
        if file.is_file():
            pathlib.Path.unlink(file, missing_ok=True)

    f = open(dir_name + file_name, 'x')
    f.write(content)
    f.close()
