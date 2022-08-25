import argparse 
from S3Client import S3Client


def s3_download(key, output_file):
    s3_client = S3Client()
    s3_client.download('mtcachedata', key, output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download a file from S3 store")
    parser.add_argument("key", 
                            help="Key to S3 object")
    parser.add_argument("output_file",
                            help="Path to store the S3 object")
    args = parser.parse_args()

    s3_download(
        args.key,
        args.output_file
    )