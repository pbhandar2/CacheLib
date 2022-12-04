import argparse 
import pandas as pd 

import boto3
from botocore.exceptions import ClientError

from Config import Config 
from ExperimentOutput import ExperimentOutput, ExperimentSet

LIVE_MACHINE_LIST = ['cl_hdd_0_10', 'cl_hdd_0_26', 'cl_hdd_0_27',
                        'cl_hdd_0_28', 'cl_hdd_0_29', 'cl_hdd_0_30',
                        'cl_hdd_0_31', 'cl_hdd_0_32', 'cl_hdd_0_33',
                        'cl_hdd_0_34', 'cl_hdd_0_35', 'cl_hdd_1_0', 
                        'cl_hdd_1_1', 'cl_hdd_1_2', 'cl_hdd_1_3', 
                        'cl_hdd_1_4', 'cl_hdd_1_5', 'cl_hdd_1_6', 
                        'cl_hdd_1_7', 'cl_hdd_1_8', 'cl_hdd_1_9', 
                        'cl_hdd_1_10', 'cl_hdd_1_11', 'cl_hdd_1_12', 
                        'cc_sh_00', 'cc_c04_0', 'cc_c04_1',
                        'cc_c04_2', 'cc_c04_3']


""" This class deletes any pending experiment that 
    is running on a machine that is not in the list 
    of live machines. 

    Args:
        aws_key: AWS key 
        aws_secret: AWS secret 
"""
class CleanExp:
    def __init__(self, aws_key: str, aws_secret: str):
        self.s3_bucket_name = "mtcachedata"
        self.key_prefix = "output_dump"

        self.s3 = boto3.client('s3',
                                aws_access_key_id=aws_key, 
                                aws_secret_access_key=aws_secret)
        
        self.key_df = self._load_keys()
        self.config = Config(aws_key, aws_secret)


    def _download_s3_obj(self, key: str, output_path: str):
        try:
            self.s3.download_file(self.config.s3_bucket_name, 
                                    key, 
                                    output_path)
        except ClientError as e:
            raise ValueError("{}::(Error downloading object at {} with key {})".format(e, output_path, key))


    def _delete_s3_obj(self, key: str):
        try:
            self.s3.delete_object(Bucket=self.config.s3_bucket_name, 
                                    Key=key)
        except ClientError as e:
            raise ValueError("{}::(Error deleting object with key {})".format(e, key))


    def _load_keys(self):
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.s3_bucket_name, Prefix=self.key_prefix)
        key_list = []
        for page in pages:
            for obj in page['Contents']:
                key_list.append(obj)

        key_df = pd.DataFrame(key_list)
        return key_df


    def custom_c220g1_delete(self):
        pass 


    def check_tag(self, tag):
        tag_flag = False 
        for live_machine_tag in LIVE_MACHINE_LIST:
            if live_machine_tag in tag:
                tag_flag = True 
                break 
        return tag_flag


    def load_pending_exp(self):
        dead_exp_list, live_exp_list, error_exp_list = [], [], []
        # iterate through all keys with low size 
        # the pending experiment have only the config uploaded which 
        # is around 1500 bytes so using threshold of 2000
        for _, row in self.key_df[self.key_df['Size']<2000].iterrows():
            # download and load the experiment output file 
            temp_file_path = self.config.get_temp_file_path_from_key(row['Key'])
            self._download_s3_obj(row['Key'], str(temp_file_path))
            exp_output = ExperimentOutput(temp_file_path)

            # load experiment info 
            exp_config = exp_output.get_exp_config()
            exp_config["machine"] = row['Key'].split("/")[1]
            exp_config["workload"] = row['Key'].split("/")[2]
            exp_config["size"] = row['Size']

            if exp_output.get_error():
                with temp_file_path.open('r') as f:
                    print(f.read())
                self._delete_s3_obj(row['Key'])
                print("Deleted {} with tag {}".format(row['Key'], exp_config['tag']))
                error_exp_list.append(exp_config)
                continue 

            # if that machine is not live we can delete this key and its content 
            if not self.check_tag(exp_config['tag']):
                # delete the key from S3 
                with temp_file_path.open('r') as f:
                    print(f.read())
                self._delete_s3_obj(row['Key'])
                print("Deleted {} with tag {}".format(row['Key'], exp_config['tag']))

                dead_exp_list.append(exp_config)
            else:
                live_exp_list.append(exp_config)

            # save the info and clean up the file 
            temp_file_path.unlink()

        dead_df = pd.DataFrame(dead_exp_list)
        dead_df.to_csv("dead.csv", index=False)

        live_df = pd.DataFrame(live_exp_list)
        live_df.to_csv("live.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This script runs experiments to find \
                                                    minimum T2 size to improve performance \
                                                    for a given workload, machine, app, step size \
                                                    and t1 size.")

    parser.add_argument("awsKey",
                            help="AWS access key")
    
    parser.add_argument("awsSecret",
                            help="AWS secret key")

    args = parser.parse_args()

    cleaner = CleanExp(args.awsKey, args.awsSecret)
    cleaner.load_pending_exp()