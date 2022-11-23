from Config import Config 
import logging 
import argparse 
import copy 
import math 
import json 
import numpy as np 
import pandas as pd 
import boto3
from botocore.exceptions import ClientError
import subprocess


class Runner:
    def __init__(self, machine, tag, aws_key, aws_secret):

        self.machine = machine 
        self.tag = tag 
        self.s3 = boto3.client('s3',
                                aws_access_key_id=aws_key, 
                                aws_secret_access_key=aws_secret)
        self.config = Config(aws_key, aws_secret)


    def get_block_trace_path(self, workload):
        return self.config.temp_storage_dir.joinpath("block_{}.csv".format(workload))


    def download_s3_obj(self, key, output_path):
        try:
            self.s3.download_file(self.config.s3_bucket_name, 
                                    key, 
                                    output_path)
        except ClientError as e:
            raise ValueError("{}::(Error downloading object at {} with key {})".format(e, output_path, key))
    

    def upload_s3_obj(self, key, input_path):
        try:
            self.s3.upload_file(input_path,
                                    self.config.s3_bucket_name, 
                                    key)
        except ClientError as e:
            raise ValueError("{}::(Error uploading object from {} with key {})".format(e, input_path, key))


    def get_key_size(self, key):
        list_api_return = self.s3.list_objects_v2(Bucket=self.config.s3_bucket_name, Prefix=key)

        if (list_api_return['KeyCount'] == 0):
            return 0 
        else:
            return int(list_api_return['Contents'][0]['Size'])


    def get_raw_output_path(self, machine,
                            workload, 
                            queue_size, 
                            thread_count,
                            iat_scale_factor,
                            t1_size_mb, 
                            t2_size_mb,
                            cur_itr):
        
        return self.config.raw_data_dir.joinpath("{}/{}/{}_{}_{}_{}_{}_{}".format(machine,
                                                                                    workload,
                                                                                    queue_size,
                                                                                    thread_count,
                                                                                    iat_scale_factor,
                                                                                    t1_size_mb,
                                                                                    t2_size_mb,
                                                                                    cur_itr))

    


    
    def download_block_trace(self, workload):
        block_trace_path = self.get_block_trace_path(workload)
        if not block_trace_path.is_file():
            print("Block trace file does not exist {} downloading ..".format(block_trace_path))
            self.download_s3_obj("block_trace/{}.csv".format(workload), str(block_trace_path))
            print("Block trace downlaoded!")
        return block_trace_path 


    def generate_config_file(self, 
                                workload, 
                                queue_size, 
                                thread_count,
                                iat_scale_factor,
                                tag,
                                t1_size, 
                                t2_size):
        
        block_trace_path = self.get_block_trace_path(workload)
        config = {}
        if t2_size == 0:
            with self.config.config_template_dir.joinpath("st_base.json").open("r") as f:
                config = json.load(f)
            config["cache_config"]["cacheSizeMB"] = t1_size 
        else:
            with self.config.config_template_dir.joinpath("mt_base.json").open("r") as f:
                config = json.load(f)
            config["cache_config"]["cacheSizeMB"] = t1_size 
            config["cache_config"]["nvmCacheSizeMB"] = t2_size
            config["cache_config"]["nvmCachePaths"] = str(self.config.nvm_file_path)
        
        config["test_config"]["diskFilePath"] = str(self.config.disk_file_path)
        config["test_config"]["maxDiskFileOffset"] = self.config.disk_file_path.expanduser().stat().st_size
        config["test_config"]["inputQueueSize"] = queue_size
        config["test_config"]["processorThreadCount"] = thread_count
        config["test_config"]["asyncIOTrackerThreadCount"] = thread_count
        config["test_config"]["scaleIAT"] = iat_scale_factor
        config["test_config"]["tag"] = tag 
        config["test_config"]["replayGeneratorConfig"]["traceList"] = [str(block_trace_path.absolute())]

        with self.config.config_file_path.open("w+") as f:
            json.dump(config, f, indent=4)

    
    def check_raw_output_file(self, 
                                machine,
                                workload, 
                                queue_size, 
                                thread_count,
                                iat_scale_factor,
                                t1_size_mb, 
                                t2_size_mb,
                                cur_itr):
        
        return self.get_raw_output_path(machine,
                                            workload,
                                            queue_size,
                                            thread_count, 
                                            iat_scale_factor,
                                            t1_size_mb,
                                            t2_size_mb,
                                            cur_itr).exists()
        
    
    def run(self, exp_config, data_only=False):
        # Run a block replay experiment 
        machine, workload = exp_config["machine"], exp_config["workload"]
        queue_size, thread_count = exp_config["queue_size"], exp_config["thread_count"]
        iat_scale, t1_size_mb = exp_config["iat_scale"], exp_config["t1_size_mb"]
        t2_size_mb, tag, it = exp_config["t2_size_mb"], exp_config["tag"], exp_config["it"]

        # download the block trace file it it has not been done yet 
        if not data_only:
            self.download_block_trace(workload)

            self.generate_config_file(workload, 
                                        queue_size, 
                                        thread_count,
                                        iat_scale,
                                        tag,
                                        t1_size_mb, 
                                        t2_size_mb)
        
        upload_key = self.config.get_upload_key(exp_config)
        
        key_size = self.get_key_size(upload_key)

        # if an output file already exists, no point checking 
        if key_size == 0:

            if not data_only:
                # upload item (lock) and run experiment 
                self.upload_s3_obj(upload_key, str(self.config.config_file_path))

                print("Running\n")
                print(json.dumps(exp_config, indent=4))

                f = self.config.raw_output_file_path.open("w+")

                # run the experiment 
                p1 = subprocess.run([self.config.run_cachebench_cmd, "--json_test_config", str(self.config.config_file_path)],
                                    stdout=f)
                
                f.close()

                # upload the output 
                self.upload_s3_obj(upload_key, str(self.config.raw_output_file_path))

                return self.get_key_size(upload_key)

        return key_size 


    def run_custom_tier_sizes(self, data_only=False):
        exp_status_list = []
        custom_tier_size_data_dir = self.config.custom_tier_size_data_dir
        for custom_tier_size_file in custom_tier_size_data_dir.iterdir():
            custom_tier_sizes_df = pd.read_csv(custom_tier_size_file)

            # group the experiments based on workload to minimize time spent 
            # downloading/removing block traces 
            for workload, per_workload_df in custom_tier_sizes_df.groupby("workload"):

                for _, custom_tier_sizes_row in per_workload_df.iterrows():
                    exp_config = self.config.get_default_app_config()
                    exp_config["machine"] = self.machine 
                    exp_config["tag"] = self.tag 
                    exp_config["workload"] = workload
                    exp_config["t1_size_mb"] = custom_tier_sizes_row["t1_size_mb"]
                    exp_config["t2_size_mb"] = custom_tier_sizes_row["t2_size_mb"]

                    for cur_itr in range(self.config.itr_count):
                        exp_config["it"] = cur_itr
                        status = self.run(exp_config, data_only=data_only)
                        exp_config["status"] = status 
                        exp_status_list.append(copy.deepcopy(exp_config))
        
        status_df = pd.DataFrame(exp_status_list)
        self.print_tier_sizes_status()
        return status_df
    

    def print_tier_sizes_status(self):
        status_df = self.run_custom_tier_sizes(data_only=True)
        print(status_df.nsmallest(25, 'status'))
        print(status_df[status_df["status"]<2000].nlargest(25, 'status'))
                        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Main script to run MT cache replay")

    parser.add_argument("machine",
                            help="Machine type identification")

    parser.add_argument("tag",
                            help="Workload identification")

    parser.add_argument("awsKey",
                            help="AWS access key")
    
    parser.add_argument("awsSecret",
                            help="AWS secret key")

    parser.add_argument("s",
                            type=bool,
                            default=False,
                            help="Print status")

    args = parser.parse_args()

    runner = Runner(args.machine, args.tag, args.awsKey, args.awsSecret)

    if args.s:
        runner.print_tier_sizes_status()
    else:
        runner.run_custom_tier_sizes()
