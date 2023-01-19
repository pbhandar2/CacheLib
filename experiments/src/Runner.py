""" The scirpt runs CacheBench block trace replay on a machine that has
the correct fork of CacheLib(pbhandar2) installed. 

Users provide the machine class, AWS credentials and experiment configuration 
file. The machine class is used to generate the output file/key. The AWS 
credentials are used to download the block trace and upload experiment output. 
The experiment configuration file is used to generate the set of experiments
to run. 

Typical usage example:

  run_block_replay = Runner(machine, experiment_config_file, aws_key, aws_secret)
  
  # run base experiments 
  run_block_replay.run()
"""


import random
import argparse 
import copy 
import math 
import time 
import socket 
import shutil
import psutil 
import itertools
import json 
import pathlib 
import subprocess
import sys 
import numpy as np 
import pandas as pd 
import boto3
from botocore.exceptions import ClientError


class Runner:
    def __init__(self, machine, experiment_name, experiment_config_file, aws_key, aws_secret):
        """ Class that runs cachebench experiments from an experiment configuration file

            Parameters
            ----------
            machine : str 
                the name of the machine 
            experiment_name : str 
                the name of the experiment 
            experiment_config_file : pathlib.Path 
                path to the JSON experiment config file 
            aws_key : str 
                the key to S3 buckets to upload and download experiment data 
            aws_secret : str 
                the secret to S3 buckets to upload and download experiment data 
        """

        # NOTE: HARDCODED the number of iteration per experiment and sleep time between 
        # tracking memory and cpu usage and possibly, uploading an updated experiment output 
        self.iteration_count = 3 
        self.usage_snapshot_window = 300 

        # NOTE: HARDCODED using the shared memory as a directory for experiment output 
        self.runner_dir = pathlib.Path("/dev/shm/cachelib-runner")

        if not self.runner_dir.exists():
            self.runner_dir.mkdir()

        self.cachebench_config_file_path = self.runner_dir.joinpath("cachebench_config.json")
        self.output_file_path = self.runner_dir.joinpath("current_experiment_output.txt")
        self.memory_cpu_usage_file_path = self.runner_dir.joinpath("memory_cpu_usage.csv")
        self.run_cachebench_cmd = "../../opt/cachelib/bin/cachebench"

        self.experiment_name = experiment_name
        self.machine = machine 

        with open(experiment_config_file, "r") as f:
            self.experiment_config_dict = json.load(f)
        
        self.config = self.experiment_config_dict[self.experiment_name]
        self.machine_details = self.experiment_config_dict["machine"][machine]
        self.t1_size_limit_gb = self.machine_details["max_t1_size"]
        self.t2_size_limit_gb = self.machine_details["max_t2_size"]

        self.s3 = None 
        if len(aws_key) > 0 and len(aws_secret) > 0:
            self.s3 = boto3.client('s3',
                                    aws_access_key_id=aws_key, 
                                    aws_secret_access_key=aws_secret)

        self.tag = socket.gethostname()

        # TODO: cleanup this mess 
        """ Only reset the experiment directory if we are using S3. This means that
            we will be downloading traces from S3 and the directory is reset to 
            remove all relevant files of an experiment once it is uploaded to S3. 
            This will prevent the shared memory directory increasing too much in size. 
            If we are not using S3, this can serve as an input directory for traces
            and also where experiment output will be stored. """
        # if self.s3 is not None:
        #     self.reset_experiment_dir()

    
    def reset_experiment_dir(self):
        if self.runner_dir.exists():
            self.runner_dir.rmdir()
        self.runner_dir.mkdir()


    def get_block_trace_path(self, workload):
        return self.runner_dir.joinpath("{}.csv".format(workload))


    def download_s3_obj(self, key, output_path):
        try:
            self.s3.download_file(self.experiment_config_dict["bucket_name"], 
                                    key, 
                                    output_path)
        except ClientError as e:
            raise ValueError("{}::(Error downloading object at {} with key {})".format(e, output_path, key))
    

    def upload_s3_obj(self, key, input_path):
        try:
            self.s3.upload_file(input_path,
                                    self.experiment_config_dict["bucket_name"], 
                                    key)
        except ClientError as e:
            raise ValueError("{}::(Error uploading object from {} with key {})".format(e, input_path, key))

    
    def delete_s3_obj(self, key):
        try:
            return self.s3.delete_object(Bucket=self.experiment_config_dict["bucket_name"], Key=key)
        except ClientError as e:
            raise ValueError("{}::(Error deleting key {}".format(e, key))


    def get_key_size(self, key):
        list_api_return = self.s3.list_objects_v2(Bucket=self.experiment_config_dict["bucket_name"], Prefix=key)

        if (list_api_return['KeyCount'] == 0):
            return 0 
        else:
            return int(list_api_return['Contents'][0]['Size'])

    
    def download_block_trace(self, block_trace_path):
        workload = pathlib.Path(block_trace_path).stem
        if not block_trace_path.is_file():
            print("Block trace file does not exist {} downloading ..".format(block_trace_path))
            self.download_s3_obj("block_trace/{}.csv".format(workload), str(block_trace_path))
            print("Block trace downlaoded!")
        else:
            # TODO: check if the size of the block trace file matches the size of the key in S3 
            pass 
        return block_trace_path 


    def get_base_experiment_df(self):
        """ From the list of parameters, workloads and tier sizes listed in the 
            experiment configuration file for experiments titled "base_experiment". 

            Return
            ------
            df : pandas.DataFrame 
                pandas DataFrame where each row represents an experiments that needs to be run 
        """

        all_experiment_params = itertools.product(*[self.config["clean_regions"],
                                                        self.config["thread_count"],
                                                        self.config["replay_rate"],
                                                        self.config["queue_size"],
                                                        self.config["sample_ratio"],
                                                        self.config["admission_probability"],
                                                        self.config["max_t2_write_rate"],
                                                        self.config["reject_first_entry_split_count"]])
        
        experiment_param_df = pd.DataFrame(all_experiment_params, columns=["clean_regions", 
                                                                                "thread_count",
                                                                                "replay_rate", 
                                                                                "queue_size", 
                                                                                "sample_ratio",
                                                                                "admission_probability", 
                                                                                "max_t2_write_rate",
                                                                                "reject_first_entry_split_count"])
        
        tier_sizes_dict_list = []
        for workload in self.config["tier_sizes"]:
            for tier_sizes in self.config["tier_sizes"][workload]:
                t1_size_mb, t2_size_mb = tier_sizes[0], tier_sizes[1]
                tier_sizes_dict_list.append({
                    "workload": workload,
                    "t1_size": t1_size_mb, 
                    "t2_size": t2_size_mb
                })
        tier_sizes_df = pd.DataFrame(tier_sizes_dict_list)
        df = pd.merge(tier_sizes_df, experiment_param_df, how='cross')
        
        # NOTE: filtering all the points any two or more of max_t2_write_rate 
        # , admission_probability or reject_first_entry_split_count 
        # because it is not yet supported in cachelib 
        drop_index = df[(df['max_t2_write_rate']>0) & \
                            (df['admission_probability']>0) & \
                                (df["reject_first_entry_split_count"]>0)].index
        df.drop(drop_index, inplace=True)

        # NOTE: cant set dynamic random device limit and static random admission 
        drop_index = df[(df['max_t2_write_rate']>0) & (df['admission_probability']>0)].index
        df.drop(drop_index, inplace=True)

        # NOTE: cant set dynamic random device limit and reject first entry split count 
        drop_index = df[(df['max_t2_write_rate']>0) & (df["reject_first_entry_split_count"]>0)].index
        df.drop(drop_index, inplace=True)

        # NOTE: cant set static random admission probability and reject first entry split count 
        drop_index = df[(df['admission_probability']>0) & (df["reject_first_entry_split_count"]>0)].index
        df.drop(drop_index, inplace=True)
        
        # NOTE: not doing any tier-2 admission for sampled traces yet 
        drop_index = df[(df['sample_ratio']>0) & \
                    (((df['max_t2_write_rate']>0) | \
                        (df['admission_probability']>0) | \
                            (df["reject_first_entry_split_count"]>0)))].index 
        df.drop(drop_index, inplace=True)

        return df 


    def get_config(self, s3_prefix, experiment_params, it):
        """ Get the configuration dict and output keys for a given 
            experiment. 

            Parameters
            ----------
            s3_prefix : str 
                the S3 prefix to use in the key 
            experiment_params : pd.Series 
                pandas Series containing experiment parameters 
            it : int 
                the iteration count of the experiment 
            
            Return 
            ------
            config: dict 
                a dict containing the experiment configuration to pass to cachebench 
            key_dict : dict 
                a dict containing relevant keys to the experiment 
        """

        key_dict = {}
        for status_str in ["live", "done", "error", "usage"]:
            key_dict[status_str] = "{}/{}/{}/{}/{}_{}_{}_{}_{}_{}_{}_{}_{}_{}_{}".format(
                                        s3_prefix,
                                        status_str,
                                        self.machine, 
                                        experiment_params['workload'],
                                        experiment_params['queue_size'],
                                        experiment_params['thread_count'],
                                        experiment_params['replay_rate'],
                                        experiment_params['t1_size'],
                                        experiment_params['t2_size'],
                                        it,
                                        int(100*experiment_params['sample_ratio']),
                                        experiment_params['clean_regions'],
                                        int(100*experiment_params['admission_probability']),
                                        experiment_params['max_t2_write_rate'],
                                        experiment_params["reject_first_entry_split_count"]) 

        block_trace_path = self.get_block_trace_path(experiment_params['workload'])
        config = {
            "cache_config": {},
            "test_config": {}
        }
        config["cache_config"]["cacheSizeMB"] = experiment_params['t1_size'] 
        config["cache_config"]["lruUpdateOnWrite"] = True 
        config["cache_config"]["allocSizes"] = [4136]
        
        if experiment_params['t2_size'] > 0:
            config["cache_config"]["nvmCacheSizeMB"] = experiment_params['t2_size'] 
            config["cache_config"]["nvmCachePaths"] = self.machine_details["nvm_file_path"]
            config["cache_config"]["navySizeClasses"] = []
            config["cache_config"]["navyBigHashSizePct"] = 0
            config["cache_config"]["navyBlockSize"] = 4096
            config["cache_config"]["truncateItemToOriginalAllocSizeInNvm"] = True 
            config["cache_config"]["printNvmCounters"] = True 

            if experiment_params['admission_probability'] > 0:
                config["cache_config"]["navyAdmissionProbability"] = experiment_params['admission_probability']
            
            if experiment_params['max_t2_write_rate'] > 0:
                config["cache_config"]["navyMaxDeviceWriteRateMB"] = experiment_params["max_t2_write_rate"]
            
            if experiment_params['reject_first_entry_split_count'] > 0:
                config["cache_config"]["navyRejectFirstAPEntryCount"] = experiment_params['reject_first_entry_split_count']
                config["cache_config"]["navyRejectFirstAPSplitCount"] = experiment_params['reject_first_entry_split_count']
                config["cache_config"]["navyRejectFirstAPDRAMHints"] = True 
            
            config["cache_config"]["navyCleanRegions"] = experiment_params['clean_regions']

        config["test_config"]["populateItem"] = True 
        config["test_config"]["generator"] = "multi-replay"
        config["test_config"]["numThreads"] = 1
        config["test_config"]["inputQueueSize"] = experiment_params['queue_size']
        config["test_config"]["processorThreadCount"] = experiment_params['thread_count']
        config["test_config"]["asyncIOTrackerThreadCount"] = experiment_params['thread_count']
        config["test_config"]["traceBlockSizeBytes"] = 512
        config["test_config"]["pageSizeBytes"] = 4096
        config["test_config"]["statPrintDelaySec"] = 30
        config["test_config"]["relativeTiming"] = True 
        config["test_config"]["scaleIAT"] = experiment_params['replay_rate']
        config["test_config"]["diskFilePath"] = self.machine_details["disk_file_path"]
        config["test_config"]["maxDiskFileOffset"] = pathlib.Path(self.machine_details["disk_file_path"]).expanduser().stat().st_size
        config["test_config"]["replayGeneratorConfig"] = {
            "traceList": [str(block_trace_path.absolute())]
        }
        config["test_config"]["tag"] = self.tag

        return config, key_dict


    def run_base_experiments(self):
        """ Run the base experiments from the configuration file 
        """

        experiment_df = self.get_base_experiment_df()
        s3_key_prefix = self.config["s3_prefix"]
        for experiment_index, experiment_row in experiment_df.iterrows():
            for it in range(self.iteration_count):
                # setup the configuration file 
                config_json, key_dict = self.get_config(s3_key_prefix, experiment_row, it)

                if (experiment_row ["t1_size"]/1e3) > self.t1_size_limit_gb or \
                        (experiment_row["t2_size"]/1e3) > self.t2_size_limit_gb:
                    status = -1 
                else:
                    status = self.run_main(config_json, s3_key_prefix, key_dict)
                    if status == -1:
                        print("An experiment failed. Check the runner output directory.")
                        return 


    def check_experiment_status(self, key_dict):
        """ Check if an experiment has already started running in some
            machine or if it is already done. 

            Parameters
            ----------
            key_dict : dict 
                dict containing the live and done key to check 
            
            Return 
            ------
            experiment_has_already_started_flag : bool
                a flag denoting whether the experiment has already started or completed 
        """

        experiment_has_already_started_flag = False 
        for experiment_status in key_dict:
            cur_s3_key = key_dict[experiment_status]
            s3_object_size = self.get_key_size(cur_s3_key)
            if s3_object_size > 0:
                experiment_has_already_started_flag = True 
                break 
        return experiment_has_already_started_flag

        
    def snap_system_stats(self, usage_handle):
        """ Snap the memory, cpu and disk stats and write it to a logfile. 
        """

        mem_stats = psutil.virtual_memory()

        column_list, value_list = [], []
        for mem_stat_field in mem_stats._fields:
            mem_stat_value = getattr(mem_stats, mem_stat_field)
            if type(mem_stat_value) == str:
                column_list.append(mem_stat_field)
                value_list.append(mem_stat_value)

        cpu_stats = psutil.cpu_percent(percpu=True)
        cpu_stat_name = ["cpu_util_{}".format(_) for _ in range(len(cpu_stats))]

        column_list += cpu_stat_name
        value_list += cpu_stats 

        output_str = ",".join([str(_) for _ in value_list])
        usage_handle.write("{}\n".format(output_str))
        
        
    def run_main(self, cachebench_config, s3_prefix, key_dict):
        """ The main function that runs CacheBench using subprocess and tracks its progress.

        Parameters
        ----------
        cachebench_config : dict 
            a dict containing the cachebench configuration of the experiment to be run 
        s3_prefix : str 
            the name of the root bucket of the experiment  
        key_dict : dict
            a dict containing different keys for 'live', 'done' and 'error' experiment status
        
        Return
        ------
        size : int 
            the size of the upload key 
        """

        experiment_running_flag = False 
        if self.s3 is not None:
            if self.check_experiment_status(key_dict):
                return 0 

        # load the configuration file for CacheBench 
        with self.cachebench_config_file_path.open("w+") as f:
            json.dump(cachebench_config, f, indent=4)
            
        # upload the config to s3 to lock the experiment to avoid other 
        # machines also running the same experiment and download the block
        if self.s3 is not None:
            self.upload_s3_obj(key_dict['live'], str(self.cachebench_config_file_path))
            self.download_block_trace(pathlib.Path(cachebench_config["test_config"]["replayGeneratorConfig"]["traceList"][0]))

        # run the experiment and track its memory, CPU usage
        # fail smoothly if block replay failed 
        start_time = time.time()
        with self.output_file_path.open("w+") as output_handle, \
                self.memory_cpu_usage_file_path.open("w+") as usage_handle:
                
            # run the cachelib experiment 
            with subprocess.Popen([self.run_cachebench_cmd, 
                                    "--json_test_config", 
                                    str(self.cachebench_config_file_path)], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.STDOUT, 
                                    bufsize=1,
                                    encoding='utf-8') as p:
                    
                for line in p.stdout: # b'\n'-separated lines
                    sys.stdout.buffer.write(bytes(line, 'utf-8')) # pass bytes as is
                    output_handle.write(line)
                    cur_time = time.time()
                    if (cur_time - start_time) > self.usage_snapshot_window:
                        self.snap_system_stats(usage_handle)
                        start_time = cur_time 
                
                # wait for it to complete 
                p.wait()
                
                if p.returncode == 0:
                    if self.s3 is not None:
                        self.upload_s3_obj(key_dict['done'], str(self.output_file_path))
                        self.upload_s3_obj(key_dict['usage'], str(self.memory_cpu_usage_file_path))
                        self.delete_s3_obj(key_dict['live'])
                    return 1 
                else:
                    if self.s3 is not None:
                        self.upload_s3_obj(key_dict['error'], str(self.output_file_path))
                        self.delete_s3_obj(key_dict['live'])
                    return -1 


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""The scirpt runs CacheBench block trace replay 
                                                    on a machine that has the correct fork of 
                                                    CacheLib(pbhandar2) installed. """)

    parser.add_argument("machine",
                            help="Machine class identification")
    
    parser.add_argument("experiment_name",
                            help="Name of the experiment (also a field in the experiment config)")

    parser.add_argument("experimentConfigFile",
                            help="Experiment configuration file")

    parser.add_argument("--awsKey",
                            default='',
                            help="AWS access key")
    
    parser.add_argument("--awsSecret",
                            default='',
                            help="AWS secret key")

    args = parser.parse_args()
    runner = Runner(args.machine, args.experiment_name, args.experimentConfigFile, args.awsKey, args.awsSecret)
    runner.run_base_experiments()