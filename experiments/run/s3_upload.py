import argparse 
from S3Client import S3Client


def s3_upload(key, output_file):
    s3_client = S3Client()
    s3_client.upload(key, output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a file to S3 store")
    parser.add_argument("key", 
                            help="Key to S3 object")
    parser.add_argument("input_file",
                            help="Path to file to upload")
    args = parser.parse_args()

    s3_upload(
        args.key,
        args.input_file
    )