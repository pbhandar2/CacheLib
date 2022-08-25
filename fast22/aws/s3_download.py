import argparse 
import pathlib 
from S3Client import S3Client


def s3_download(key, output_file_path):
    s3_client = S3Client()
    s3_client.download('mtcachedata', key, str(output_file_path.resolve()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download a file from S3 store")
    parser.add_argument("key", 
                            help="Key to S3 object")
    parser.add_argument("output_file_path",
                            type=pathlib.Path,
                            help="Path to file where the S3 object data is stored")
    args = parser.parse_args()

    s3_download(
        args.key,
        args.output_file_path
    )