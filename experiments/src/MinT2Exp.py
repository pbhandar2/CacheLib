import math 
import argparse 
import pathlib 
import copy 
import shutil

import pandas as pd 
import numpy as np 

import boto3
from botocore.exceptions import ClientError

from Config import Config 
from Runner import Runner
from ExperimentOutput import ExperimentOutput, ExperimentSet


class MinT2Exp:
    def __init__(self, machine, tag, aws_key, aws_secret):
        self.machine = machine
        self.tag = tag 
        self.s3 = boto3.client('s3',
                                aws_access_key_id=aws_key, 
                                aws_secret_access_key=aws_secret)
        self.config = Config(aws_key, aws_secret)
        self.runner = Runner(machine, tag, aws_key, aws_secret)


    def download_s3_obj(self, key, output_path):
        try:
            self.s3.download_file(self.config.s3_bucket_name, 
                                    key, 
                                    output_path)
        except ClientError as e:
            raise ValueError("{}::(Error downloading object at {} with key {})".format(e, output_path, key))


    def get_key_size(self, key):
        list_api_return = self.s3.list_objects_v2(Bucket=self.config.s3_bucket_name, Prefix=key)

        if (list_api_return['KeyCount'] == 0):
            return 0 
        elif (list_api_return['KeyCount'] == 1):
            return int(list_api_return['Contents'][0]['Size'])
        else:
            raise ValueError("Multiple keys mapping to provided key, which size to return?")


    def get_temp_file_name(self, exp_config):
        return "{}_{}_{}_{}_{}_{}_{}_{}".format(exp_config["machine"],
                                                    exp_config["workload"],
                                                    exp_config["queue_size"],
                                                    exp_config["thread_count"],
                                                    exp_config["iat_scale"],
                                                    exp_config["t1_size_mb"],
                                                    exp_config["t2_size_mb"],
                                                    exp_config["it"])


    def get_temp_file_path(self, exp_config):
        return self.config.temp_storage_dir.joinpath(self.get_temp_file_name(exp_config))


    def download_temp_output_file(self, exp_config):
        exp_key = self.config.get_upload_key(exp_config)

        if self.get_key_size(exp_key):
            temp_file_path = self.get_temp_file_path(exp_config)

            # download this key to this file 
            self.download_s3_obj(exp_key, str(temp_file_path))

            return temp_file_path 
        else:
            return None 

    
    def load_temp_output_file(self, exp_config):
        # download the output file if the experiment output exists 
        temp_output_file_path = self.download_temp_output_file(exp_config)

        if temp_output_file_path is None:
            # experiment output does not exist so we need to run it 
            self.runner.run(exp_config)

            # copy the current output file to the local folder 
            temp_output_file_path = self.get_temp_file_path(exp_config)
            shutil.copy(str(self.config.raw_output_file_path), str(temp_output_file_path))
        else:
            # experiment output exists, now evaluate if experiment is complete 
            exp_output = ExperimentOutput(temp_output_file_path)
            if not exp_output.is_output_complete():
                # experiment is live 
                temp_output_file_path = None 
        
        return temp_output_file_path
            

    def eval_set(self, exp_config):
        exp_output_path_list = []
        for it in range(self.config.it_limit):
            exp_config["it"] = it 
            load_result = self.load_temp_output_file(exp_config)

            if load_result is None:
                return None 
            else:
                exp_output_path_list.append(load_result)
        else:
            return ExperimentSet(exp_output_path_list)


    def get_partial_key(self, exp_config):
        return "output_dump/{}/{}/{}_{}_{}_{}".format(exp_config["machine"],
                                                        exp_config["workload"],
                                                        exp_config["queue_size"],
                                                        exp_config["thread_count"],
                                                        exp_config["iat_scale"],
                                                        exp_config["t1_size_mb"])


    def load_key_data(self, exp_config):
        partial_key = self.get_partial_key(exp_config)
        list_api_return = self.s3.list_objects_v2(Bucket=self.config.s3_bucket_name, Prefix=partial_key)
        key_count = list_api_return['KeyCount']
        key_data = []
        if key_count > 0:
            contents = list_api_return['Contents']
            for api_return_item in contents:
                key_data.append({"key": api_return_item['Key'], "size": api_return_item['Size']})
        
        df = pd.DataFrame(key_data)
        return df 


    def check_if_locked(self, exp_config):
        lock = False
        key_df = self.load_key_data(exp_config)
        if len(key_df) > 0 and len(key_df[key_df['size']<2000]) > 0:
            """ The output files of a locked/incomplete experiment 
                have a small size. This is because the output 
                only contains the configuration file and only 
                after the experiment is complete the entire 
                output is uploaded which is much larger. 
            """
            lock = True 
        return lock 


    def create_exp_config(self, workload, t1_size_mb, t2_size_mb):
        exp_config = self.config.get_default_app_config()
        exp_config["machine"], exp_config["workload"] = self.machine, workload 
        exp_config["t1_size_mb"], exp_config["t2_size_mb"] = t1_size_mb, t2_size_mb
        exp_config["tag"] = self.tag
        return exp_config


    def run_custom_files(self, step_size_gb=1):
        for custom_exp_list_file in self.config.min_t2_exp_dir.iterdir():
            # File name represents a workload (e.g. w97.csv)
            workload = custom_exp_list_file.stem 

            # list of statuses of each tier 1 size evaluated 
            experiment_status_list = []

            with custom_exp_list_file.open("r") as f:
                """ Each line of the custom experiment list file 
                    consists of a tier 1 size. 
                    
                    For each tier 1 size, we find the minimum 
                    tier 2 size that improces performance (MT > ST). 
                """
                line = f.readline()
                while line:
                    t1_size_mb = int(line.rstrip())

                    print("Evaluate T1: {} for workload {}".format(t1_size_mb, workload))

                    # create a experiment config for this experiment 
                    exp_config = self.create_exp_config(workload, t1_size_mb, 0)

                    # check if anything is pending and break if there is 
                    lock_flag = self.check_if_locked(exp_config)
                    if lock_flag:
                        experiment_status_list.append({"workload": workload, "t1_size": t1_size_mb, "status": 0})
                        line = f.readline()
                        continue 

                    # first make sure the ST set is done 
                    st_set = self.eval_set(exp_config)
                    if st_set is None:
                        experiment_status_list.append({"workload": workload, "t1_size": t1_size_mb, "status": 0})
                        line = f.readline()
                        continue 
                    
                    st_bandwidth = st_set.get_mean_metric("bandwidth_byte/s")

                    # now binary search to find the smallest tier-2 size 
                    # that improves performance to the closest GB 
                    t2_limit_gb = self.config.get_t2_size_limit_gb(self.machine)
                    t2_wss_gb = int(min(self.config.get_t2_wss_gb(workload), t2_limit_gb))

                    if t2_wss_gb > t2_limit_gb:
                        # if wss is larger than the tier 2 limit
                        # evaluate the largest tier 2 cache first 
                        cur = t2_limit_gb
                        low = step_size_gb
                        high = t2_limit_gb
                    elif t2_wss_gb <= step_size_gb:
                        # if wss is smaller or equal to the step size 
                        # evaluate the smallest tier 2 size first 
                        cur = step_size_gb
                        low = step_size_gb
                        high = t2_limit_gb
                    else:
                        # start by evaluating tier 2 size equal to wss
                        cur = t2_wss_gb
                        low = step_size_gb
                        high = t2_limit_gb
                    
                    lock_flag = False 
                    while True:
                        exp_config["t2_size_mb"] = int(cur *1e3)

                        # evaluate a tier-2 size and break if locked 
                        mt_set = self.eval_set(exp_config)
                        if mt_set is None:
                            lock_flag = True 
                            break 

                        # compare performance with ST 
                        mt_bandwidth = mt_set.get_mean_metric("bandwidth_byte/s")
                        if mt_bandwidth > st_bandwidth:
                            if cur == low:
                                break 
                            elif cur-low == 1:
                                cur, low, high = low, low, low 
                            else:
                                high = cur -1 
                                cur = math.floor((cur+low)/2)
                        else:
                            if cur == high:
                                break 
                            elif high-cur == 1:
                                cur, low, high = high, high, high 
                            else:
                                low = cur + 1 
                                cur = math.floor((cur+high)/2)

                    if lock_flag:
                        experiment_status_list.append({"workload": workload, "t1_size": t1_size_mb, "status": 0})
                    else:
                        experiment_status_list.append({"workload": workload, "t1_size": t1_size_mb, "status": 1})

                    line = f.readline()

            experiment_status = pd.DataFrame(experiment_status_list)
            print(experiment_status)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="This script runs experiments to find \
                                                    minimum T2 size to improve performance \
                                                    for a given workload, machine, app, step size \
                                                    and t1 size.")

    parser.add_argument("machine",
                            help="Machine type identification")

    parser.add_argument("tag",
                            help="Workload identification")

    parser.add_argument("awsKey",
                            help="AWS access key")
    
    parser.add_argument("awsSecret",
                            help="AWS secret key")

    args = parser.parse_args()

    experiment = MinT2Exp(args.machine, args.tag, args.awsKey, args.awsSecret)
    experiment.run_custom_files()