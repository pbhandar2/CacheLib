import argparse 
import pathlib 
import os
import math 
import json 
import itertools
import pandas as pd 
import boto3 
import logging 
import subprocess
from botocore.exceptions import ClientError


# user defined variables 
QUEUE_SIZE_VALUES = [32, 64, 128, 256]
THREAD_COUNT_VALUES = [8, 16, 32]
IAT_SCALE_FACTOR_VALUES = [1, 10, 100, 1000, 5000]
SIZE_MULTIPLIER_VALUES = [2, 4, 8, 16]

ITERATION_COUNT_VALUES = [3,5,10]

DEFAULT_QUEUE_SIZE = 128
DEFAULT_THREAD_COUNT = 16
DEFAULT_IAT_SCALE_FACTOR = 100 
DEFAULT_ITERATION_COUNT = 3
DEFAULT_MAX_T1_SIZE_MB = 2000 # 2 GB
DEFAULT_MIN_T1_SIZE_MB = 100 # 100 MB
DEFAULT_MEMORY_LIMIT = 40000 # 40 GB
DEFAULT_NVM_LIMIT = 400000 # 400 GB
DEFAULT_STEP_SIZE_MB = 1000 # 1 GB
DEFAULT_SIZE_MULTIPLIER = 16

DEFAULT_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
DEFAULT_AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

DEFAULT_RUN_COMMAND = "../../opt/cachelib/bin/cachebench"
DEFAULT_TAG = "unknown"
DEFAULT_BUCKET = "mtcachedata"

DEFAULT_DISK_FILE_PATH = pathlib.Path.home().joinpath("disk", "disk.file")
DEFAULT_NVM_FILE_PATH = pathlib.Path.home().joinpath("nvm", "disk.file")
DEFAULT_CONFIG_FILE_PATH = pathlib.Path.home().joinpath("temp_config.json")
DEFAULT_OUTPUT_DUMP_PATH = pathlib.Path.home().joinpath("temp_out.dump")

DEFAULT_EXTERNAL_DATA = pathlib.Path("../data/cp_block.csv")


class Runner:
    def __init__(self, args):
        self.machine_id = args.machineID
        self.workload_id = args.workloadID
        self.step_size_mb = args.stepSizeMB
        self.size_multiplier = args.sizeMultiplier
        self.iteration_count = args.iterationCount
        self.nvm_file_path = args.nvmFilePath
        self.disk_file_path = args.diskFilePath
        self.queue_size = args.queueSize
        self.thread_count = args.threadCount
        self.iat_scale_factor = args.iatScaleFactor
        self.tag = args.tag
        self.block_trace_path = pathlib.Path.home().joinpath("{}.csv".format(self.workload_id))
        self.config_file_path = args.configFilePath
        self.output_dump_path = args.outputDumpPath
        self.run_cmd = args.runCMD

        self.s3 = boto3.client('s3')
        self.bucket_name = args.bucketName

        # if block trace file does not exist, download from S3 
        if not self.block_trace_path.is_file():
            print("Block trace path does not exist {}".format(self.block_trace_path))
            try:
                self.s3.download_file(self.bucket_name, 
                                        "block_trace/{}.csv".format(self.workload_id), 
                                        str(self.block_trace_path))
            except ClientError as e:
                logging.error("Error: {} in download".format(e))

        self.args = args 
        self.external_df = None
        if args.externalData.is_file():
            self.external_df = pd.read_csv(args.externalData)
            assert self.workload_id in self.external_df["workload"].to_list()
            self.row = self.external_df[self.external_df["workload"]==self.workload_id]
            self.max_t1_size_gb = int(math.ceil(self.row["page_working_set_size"].item()/1e9))
        else:
            raise ValueError("External data not provided. Must have for now, fix later.")


    def generate_config_file(self, t1_size, t2_size):
        config = {}
        if t2_size == 0:
            with open("./config_templates/st_config_template.json") as f:
                config = json.load(f)
            config["cache_config"]["cacheSizeMB"] = t1_size 
        else:
            with open("./config_templates/mt_config_template.json") as f:
                config = json.load(f)
            config["cache_config"]["cacheSizeMB"] = t1_size 
            config["cache_config"]["nvmCacheSizeMB"] = t2_size
            config["cache_config"]["nvmCachePaths"] = str(self.nvm_file_path)
        
        config["test_config"]["diskFilePath"] = str(self.disk_file_path)
        config["test_config"]["maxDiskFileOffset"] = self.disk_file_path.expanduser().stat().st_size
        config["test_config"]["inputQueueSize"] = self.queue_size
        config["test_config"]["processorThreadCount"] = self.thread_count
        config["test_config"]["asyncIOTrackerThreadCount"] = self.thread_count
        config["test_config"]["scaleIAT"] = self.iat_scale_factor
        config["test_config"]["tag"] = self.tag 
        config["test_config"]["replayGeneratorConfig"]["traceList"] = [str(self.block_trace_path.absolute())]

        with self.config_file_path.open("w+") as f:
            json.dump(config, f, indent=4)


    def get_upload_key(self, t1_size, t2_size, cur_iteration):
        return "output_dump/{}/{}/{}_{}_{}_{}_{}_{}".format(self.machine_id, 
                                                    self.workload_id,
                                                    self.queue_size,
                                                    self.thread_count,
                                                    self.iat_scale_factor,
                                                    t1_size, 
                                                    t2_size, 
                                                    cur_iteration)

    
    def fixed_step(self):
        # start iteration from larger size caches that finishes faster 
        for tier1_size_mb in reversed(range(self.step_size_mb, self.max_t1_size_gb*1000+1, self.step_size_mb)):
            # generate all possible T2 sizes based on current T1 size and max T1 size 
            diff_from_max_tier1_size_mb = self.max_t1_size_gb*1000 - tier1_size_mb

            tier2_step_size = self.step_size_mb * self.size_multiplier
            max_tier2_size = diff_from_max_tier1_size_mb * self.size_multiplier
            tier2_size_mb_list = list(reversed(range(tier2_step_size, max_tier2_size, tier2_step_size)))
            tier2_size_mb_list.append(0)

            for tier2_size_mb in tier2_size_mb_list:
                # check if the needed T1 and T2 is possible 
                if tier1_size_mb > self.args.ramSizeMB:
                    print("Tier 1 size needed is too large! Need: {} Max: {}".format(tier1_size_mb, self.args.ramSizeMB))
                    continue 

                if tier2_size_mb > self.args.nvmSizeMB:
                    print("Tier 2 size needed is too large! Need: {} Max: {}".format(tier2_size_mb, self.args.nvmSizeMB))
                    continue 

                self.generate_config_file(tier1_size_mb, tier2_size_mb)
                for current_iteration in range(self.iteration_count):
                    upload_key = self.get_upload_key(tier1_size_mb, tier2_size_mb, current_iteration)

                    # check if key already exists 
                    if (self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=upload_key)['KeyCount'] == 0):
                        print("Key does not exist -> {}".format(upload_key))

                        # upload config to key 
                        try:
                            self.s3.upload_file(str(self.config_file_path), self.bucket_name, upload_key)
                        except ClientError as e:
                            logging.error("Error: {} in upload".format(e))

                        f = self.output_dump_path.open("w+")

                        # run the experiment 
                        p1 = subprocess.run([self.run_cmd, "--json_test_config", str(self.config_file_path)],
                                            stdout=f)

                        f.close()

                        # upload output to dump 
                        try:
                            self.s3.upload_file(str(self.output_dump_path), self.bucket_name, upload_key)
                        except ClientError as e:
                            logging.error("Error: {} in upload".format(e))

                    else:
                        print("Key exists -> {}".format(upload_key))


        def priority_mode(self):
            # run configuration based on the order in a configuration priority file 
            f = self.config_priority_file.open("r")
            config_list = json.load(f)
            for config_set in config_list:
                thread_count_list = config_set["thread_count"]
                iat_scale_factor_list = config_set["iat_scale_factor"]
                queue_size_list = config_set["queue_size"]
                size_multiplier_list = config_set["size_multiplier"]

                for block_replay_config in itertools.product(*[thread_count_list, iat_scale_factor_list, queue_size_list, size_multiplier_list]):
                    # run fixed step 
                    self.thread_count, self.iat_scale_factor = block_replay_config[0], block_replay_config[1]
                    self.queue_size, self.size_multiplier = block_replay_config[2], block_replay_config[3]
                    self.fixed_step()
            f.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Main script to run MT cache replay")

    parser.add_argument("machineID",
                            help="Machine type identification")

    parser.add_argument("workloadID",
                            help="Workload identification")

    parser.add_argument("--queueSize",
                            type=int,
                            default=DEFAULT_QUEUE_SIZE,
                            choices=QUEUE_SIZE_VALUES,
                            help="Maximum outstanding requests queued in the system")

    parser.add_argument("--threadCount",
                            type=int,
                            default=DEFAULT_THREAD_COUNT,
                            choices=THREAD_COUNT_VALUES,
                            help="Number of threads used for each of block request and async IO processing")

    parser.add_argument("--iatScaleFactor",
                            type=int,
                            default=DEFAULT_IAT_SCALE_FACTOR,
                            choices=IAT_SCALE_FACTOR_VALUES,
                            help="The value by which to divide IAT between block requests")

    parser.add_argument("--iterationCount",
                            type=int,
                            default=DEFAULT_ITERATION_COUNT,
                            choices=ITERATION_COUNT_VALUES,
                            help="Number of iterations to run per configuration")

    parser.add_argument("--maxT1CacheSizeMB",
                            default=DEFAULT_MAX_T1_SIZE_MB,
                            type=int,
                            help="Max size of tier-1 cache in MB")

    parser.add_argument("--ramSizeMB",
                            default=DEFAULT_MEMORY_LIMIT,
                            type=int,
                            help="Max memory available in the server in MB")

    parser.add_argument("--nvmSizeMB",
                            default=DEFAULT_NVM_LIMIT,
                            type=int,
                            help="Max space available in the server for NVM caching")

    parser.add_argument("--stepSizeMB",
                            type=int,
                            default=DEFAULT_STEP_SIZE_MB,
                            help="Step size in MB")

    parser.add_argument("--sizeMultiplier",
                            type=int,
                            default=DEFAULT_SIZE_MULTIPLIER,
                            choices=SIZE_MULTIPLIER_VALUES,
                            help="Size multiplier used to determine the size of tier-2 cache")

    parser.add_argument("--diskFilePath",
                            default=DEFAULT_DISK_FILE_PATH,
                            type=pathlib.Path,
                            help="Path to file on disk")
    
    parser.add_argument("--nvmFilePath",
                            default=DEFAULT_NVM_FILE_PATH,
                            type=pathlib.Path,
                            help="Path to file on NVM")

    parser.add_argument("--awsKey",
                            default=DEFAULT_AWS_ACCESS_KEY,
                            help="AWS access key")
    
    parser.add_argument("--awsSecret",
                            default=DEFAULT_AWS_SECRET_KEY,
                            help="AWS secret key")

    parser.add_argument("--runCMD",
                            default=DEFAULT_RUN_COMMAND,
                            help="The command to run cachebench")

    parser.add_argument("--externalData",
                            default=DEFAULT_EXTERNAL_DATA,
                            help="File containing external data about the workload")

    parser.add_argument("--configFilePath",
                            default=DEFAULT_CONFIG_FILE_PATH,
                            help="Path where configuration files are generated and read from")

    parser.add_argument("--outputDumpPath",
                            default=DEFAULT_OUTPUT_DUMP_PATH,
                            help="Path where the dump from experiment is stored")

    parser.add_argument("--tag",
                            default=DEFAULT_TAG,
                            help="Tag to identify specific machines within instance types")

    parser.add_argument("--bucketName",
                            default=DEFAULT_BUCKET,
                            help="Bucket name to upload results")
    
    args = parser.parse_args()

    runner = Runner(args)
    runner.priority_mode()