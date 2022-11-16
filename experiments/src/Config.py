import pathlib
import json 
import os 
import math 
import pandas as pd 
import numpy as np 
import boto3
from botocore.exceptions import ClientError



class Config:
    def __init__(self):
        self.home_dir = pathlib.Path.home()
        self.experiment_data_dir = pathlib.Path().cwd().parent.joinpath("data")

        self.raw_data_dir = self.home_dir.joinpath("mtdata")

        self.temp_storage = pathlib.Path("/dev/shm")

        # load the block features of each workload 
        self.block_data_path = self.experiment_data_dir.joinpath("cp_block.csv")
        self.block_df = pd.read_csv(self.block_data_path)

        # load details about the machines used in an experiment 
        self.machine_data_file_path = self.experiment_data_dir.joinpath("machine.json")
        with self.machine_data_file_path.open("r") as f:
            self.machine_data = json.load(f)

        # load priority of different configurations 
        self.config_priority_file_path = self.experiment_data_dir.joinpath("config_priority.json")
        with self.config_priority_file_path.open("r") as f:
            self.config_priority_data = json.load(f)

        # load overall experiment data 
        self.block_trace_dir = self.home_dir

        self.custom_t1_size_dir = self.experiment_data_dir.joinpath("custom_t1_sizes")
        self.custom_tier_size_data_dir = self.experiment_data_dir.joinpath("custom_tier_sizes")
        self.config_template_dir = self.experiment_data_dir.joinpath("config_templates")

        self.nvm_file_path = self.home_dir.joinpath("nvm", "disk.file")
        self.disk_file_path = self.home_dir.joinpath("disk", "disk.file")

        self.config_file_path = self.home_dir.joinpath("temp_config.json")
        self.raw_output_file_path = self.home_dir.joinpath("temp_out.dump")

        self.run_cachebench_cmd = "../../opt/cachelib/bin/cachebench"
        
        self.aws_access = os.getenv('AWS_ACCESS_KEY')
        self.aws_secret = os.getenv('AWS_SECRET_KEY')

        self.s3_bucket_name = "mtcachedata"
        self.base_workloads = ["w81", "w47", "w11", \
                                "w13", "w03", "w54", "w68", \
                                "w78", "w97", "w82", "w20", \
                                "w31", "w77"]

        self.s3 = boto3.client('s3')
        self.wss_pad_gb = 1
        self.t1_wss_multiplier = 1.25 
        self.t2_wss_multiplier = 1.50 
        self.itr_count = 3 
    

    def get_wss_gb(self, workload_name):
        workload_row = self.block_df[self.block_df["workload"]==workload_name].iloc[0]
        return int(math.ceil(workload_row["page_working_set_size"]/1e9)) + self.wss_pad_gb

    
    def get_custom_t1_sizes(self, workload):
        return np.genfromtxt(self.custom_t1_size_dir.joinpath("{}.csv".format(workload)), dtype=int)


    def get_upload_key(self, exp_config):
        return "output_dump/{}/{}/{}_{}_{}_{}_{}_{}".format(exp_config["machine"], 
                                                                exp_config["workload"],
                                                                exp_config["queue_size"],
                                                                exp_config["thread_count"],
                                                                exp_config["iat_scale"],
                                                                exp_config["t1_size_mb"], 
                                                                exp_config["t2_size_mb"], 
                                                                exp_config["it"])
    

    def get_t1_size_limit_mb(self, machine):
        for machine_entry in self.machine_data:
            if machine_entry["name"] == machine:
                return int(machine_entry["maxCacheSize"]*1e3)
        else:
            raise ValueError("Machine not found: {}".format(machine))


    def get_t2_size_limit_mb(self, machine):
        for machine_entry in self.machine_data:
            if machine_entry["name"] == machine:
                return int(machine_entry["maxNVMSize"]*1e3)
        else:
            raise ValueError("Machine not found: {}".format(machine))


    def get_default_app_config(self):
        return self.config_priority_data[0]

    
    def is_experiment_done(self, exp_config):
        filter_df = self.overall_df[(self.overall_df["machine"]==exp_config["machine"]) & \
                                        (self.overall_df["workload"]==exp_config["workload"]) & \
                                        (self.overall_df["queue_size"]==exp_config["queue_size"]) & \
                                        (self.overall_df["thread_count"]==exp_config["thread_count"]) & \
                                        (self.overall_df["iat_scale"]==exp_config["iat_scale"]) & \
                                        (self.overall_df["t1_size"]==exp_config["t1_size_mb"]) & \
                                        (self.overall_df["t2_size"]==exp_config["t2_size_mb"]) & \
                                        (self.overall_df["it"]==exp_config["it"])]
        
        if len(filter_df) == 0:
            # experiment not done check if it is pending 
            upload_key = self.get_upload_key(exp_config)
            if self.get_key_size(upload_key):
                return -1 
            else:
                return 0 
        else:
            return 1
        