""" This class manages the data in bucket 'mtcachedata' """

import pathlib 
import json 
import boto3
from botocore.exceptions import ClientError

class S3Client:
    def __init__(self, s3_config_file="s3_config.json"):
        self.bucket_name = "mtcachedata"

        # load the config file 
        with pathlib.Path(s3_config_file).open('r') as f:
            self.s3_config = json.load(f)
        
        self.s3 = boto3.client('s3',
                                aws_access_key_id=self.s3_config["key"], 
                                aws_secret_access_key=self.s3_config["secret"])


    def download_s3_obj(self, key, local_path):
        try:
            self.s3.download_file(self.bucket_name, key, local_path)
        except ClientError as e:
            raise ValueError("{}::(Error downloading object at {} with key {})".format(e, local_path, key))


    def upload_s3_obj(self, key, local_path):
        try:
            self.s3.upload_file(local_path, self.bucket_name, key)
        except ClientError as e:
            raise ValueError("{}::(Error uploading object from {} with key {})".format(e, local_path, key))


    def delete_s3_obj(self, key):
        try:
            return self.s3.delete_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            raise ValueError("{}::(Error deleting key {}".format(e, key))


    def get_key_size(self, key):
        list_api_return = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=key)
        if (list_api_return['KeyCount'] == 0):
            return 0 
        else:
            return int(list_api_return['Contents'][0]['Size'])
    

    def get_all_s3_content(self, prefix):
        s3_content = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page["Contents"]:
                s3_content.append(obj)
        return s3_content