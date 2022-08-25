import boto3 
import logging 
from botocore.exceptions import ClientError


class S3Client:
    def __init__(self):
        self.s3 = boto3.client('s3')

    
    def download(self, bucket, key, file_path):
        try:
            self.s3.download_file(bucket, key, file_path)
        except ClientError as e:
            logging.error("Error: {} in download".format(e))


    def upload(self, bucket, key, file_path):
        try:
            response = self.s3.upload_file(file_path, bucket, key)
        except ClientError as e:
            logging.error("Error: {} in upload".format(e))

    
    def check(self, bucket, key):
        return s3.list_objects_v2(Bucket=bucket, Prefix=key)['KeyCount']