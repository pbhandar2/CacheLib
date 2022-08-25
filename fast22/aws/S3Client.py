import boto3 


class S3Client:
    def __init__(self):
        self.s3 = boto3.client('s3')

    
    def download(self, bucket, key, file_path):
        self.s3.download_file(bucket, key, file_path)


    def upload(self, bucket, key, file_path):
        # upload a file 
        pass 