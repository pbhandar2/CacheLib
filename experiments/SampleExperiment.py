""" Replay sampled block traces in a S3 bucket for every default experiment that exists """

import pathlib 
import argparse 
import json 
import math 
import socket 

from Runner import Runner 
from S3Client import S3Client


class SampleExperiment:
    def __init__(self, machine_class, sample_rate, random_seed, bit_mask):
        self.machine_class = machine_class
        self.sample_rate = sample_rate
        self.random_seed = random_seed
        self.bit_mask = bit_mask

        self.workload_list = ['w97', 'w82', 'w11', 'w47', 'w81', 'w68']
        with open("./src/data/base_experiment_params.json") as f:
            self.exp_params = json.load(f)
            self.machine_details = self.exp_params["machine"][machine_class]

        self.s3 = S3Client()
        self.tag = socket.gethostname()


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
            s3_object_size = self.s3.get_key_size(cur_s3_key)
            if s3_object_size > 0:
                experiment_has_already_started_flag = True 
                break 
        return experiment_has_already_started_flag

    
    def generate_config(self, queue_size, thread_count, replay_rate, t1_size, t2_size, it):
        config = {
            "cache_config": {},
            "test_config": {}
        }
        config["cache_config"]["cacheSizeMB"] = t1_size 
        config["cache_config"]["lruUpdateOnWrite"] = True 
        config["cache_config"]["allocSizes"] = [4136]
        if t2_size > 0:
            config["cache_config"]["nvmCacheSizeMB"] = t2_size
            config["cache_config"]["nvmCachePaths"] = self.machine_details["nvm_file_path"]
            config["cache_config"]["navySizeClasses"] = []
            config["cache_config"]["navyBigHashSizePct"] = 0
            config["cache_config"]["navyBlockSize"] = 4096
            config["cache_config"]["truncateItemToOriginalAllocSizeInNvm"] = True 
            config["cache_config"]["printNvmCounters"] = True 
        
        config["test_config"]["populateItem"] = True 
        config["test_config"]["generator"] = "multi-replay"
        config["test_config"]["numThreads"] = 1
        config["test_config"]["inputQueueSize"] = queue_size
        config["test_config"]["processorThreadCount"] = thread_count
        config["test_config"]["asyncIOTrackerThreadCount"] = thread_count
        config["test_config"]["traceBlockSizeBytes"] = 512
        config["test_config"]["pageSizeBytes"] = 4096
        config["test_config"]["statPrintDelaySec"] = 30
        config["test_config"]["relativeTiming"] = True 
        config["test_config"]["scaleIAT"] = replay_rate
        config["test_config"]["diskFilePath"] = self.machine_details["disk_file_path"]
        config["test_config"]["maxDiskFileOffset"] = pathlib.Path(self.machine_details["disk_file_path"]).expanduser().stat().st_size
        config["test_config"]["replayGeneratorConfig"] = {
            "traceList": [self.machine_details["block_trace_path"]]
        }
        config["test_config"]["tag"] = self.tag
        return config 


    def tier_size_eligible(self, t1_size, t2_size):
        return not (t1_size < 100 or \
                        t2_size < 150 or \
                        t1_size > (self.machine_details["max_t1_size"]*1e3) or \
                        t2_size > (self.machine_details["max_t2_size"]*1e3))
    

    def run_sample_for_default_key(self, key):
        """ Run sampling experiment for a default experiment with the 
            provided key. 

            Parameters
            ----------
            key : str 
                key of the default experiment for which we run a sampling experiments 
        """
        sample_key_prefix = "experiment_output_dump/sample_default"

        # get the machine and workload where it was run 
        split_key = key.split("/")
        machine_class = split_key[3]
        workload = split_key[4]
        
        # get experiment parameters 
        split_file_name = split_key[-1].split("_")
        queue_size = int(split_file_name[0])
        thread_count = int(split_file_name[1])
        replay_rate = int(split_file_name[2])
        t1_size = int(split_file_name[3])
        t2_size = int(split_file_name[4])
        sample_t1_size = int(self.sample_rate * t1_size/100)
        sample_t2_size = int(self.sample_rate * t2_size/100)
        it = int(split_file_name[5])

        s3_key_suffix = "{}_{}_{}_{}_{}_{}_{}_{}_{}".format(queue_size, 
                                                            thread_count,
                                                            replay_rate,
                                                            t1_size, 
                                                            t2_size, 
                                                            self.sample_rate,
                                                            self.random_seed,
                                                            self.bit_mask,
                                                            it)

        key_dict = {}
        for key_type in ["live", "done", "error", "usage"]:
            s3_key_prefix = "{}/{}/{}/{}".format(sample_key_prefix, key_type, machine_class, workload)
            key_dict[key_type] = "{}/{}".format(s3_key_prefix, s3_key_suffix)

        # only run sampling for selected workloads and for experiments that 
        # belong to this machine class 
        if workload not in self.workload_list or machine_class != self.machine_class:
            return key_dict, -1

        # check if the new key already exists 
        if self.check_experiment_status(key_dict):
            return key_dict, -1

        # only run eligible tier sizes based on the machine
        if not self.tier_size_eligible(sample_t1_size, sample_t2_size):
            return key_dict, -1
        
        # generate config
        config = self.generate_config(queue_size, thread_count, replay_rate, sample_t1_size, sample_t2_size, it)
        with open(self.machine_details["cachebench_config_path"], "w+") as f:
            f.write(json.dumps(config, indent=4))
        
        # lock the experiment 
        self.s3.upload_s3_obj(key_dict['live'], self.machine_details["cachebench_config_path"])

        # download the block trace 
        block_trace_key = "sample_block_trace/{}/{}_{}_{}.csv".format(workload, self.random_seed, self.sample_rate, self.bit_mask)
        self.s3.download_s3_obj(block_trace_key, self.machine_details["block_trace_path"])
        print("block trace {} downloaded to {}".format(block_trace_key, self.machine_details["block_trace_path"]))

        print("running")
        print(config)
        runner = Runner(self.machine_details["exp_output_path"], self.machine_details["usage_output_path"])
        return_code = runner.run(self.machine_details["cachebench_config_path"])
        return key_dict, return_code


    def run(self):
        """ This functions runs a sampling experiment for each default 
            experiment that already exists. 
        """
        default_experiment_key_prefix = "experiment_output_dump/default/done/"
        s3_content = self.s3.get_all_s3_content(default_experiment_key_prefix)
        for s3_obj in s3_content:
            s3_key = s3_obj["Key"]
            key_dict, return_code = self.run_sample_for_default_key(s3_key)
            if return_code > 0:
                self.s3.upload_s3_obj(key_dict['error'], str(self.machine_details["exp_output_path"]))
                self.s3.delete_s3_obj(key_dict['live'])
                print("Error running s3 key: {}".format(s3_key))
                break 
            elif return_code == 0:
                self.s3.upload_s3_obj(key_dict['done'], str(self.machine_details["exp_output_path"]))
                self.s3.upload_s3_obj(key_dict['usage'], str(self.machine_details["usage_output_path"]))
                self.s3.delete_s3_obj(key_dict['live'])
                print("Sucessfully ran s3 key: {}".format(s3_key))
            else:
                print("Couldn't run sampling for S3 key: {}".format(s3_key))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay sampled block trace")
    parser.add_argument("machineclass", help="The class of the machine")
    parser.add_argument("--samplerate",
                            type=int,
                            default=10,
                            help="The sampling rate used to generate the sample trace")
    parser.add_argument("--randomseed",
                            type=int,
                            default=42,
                            help="The random seed used to generate the sample trace")
    parser.add_argument("--bitmask",
                            type=int,
                            default=4294901760,
                            help="The bitmask used to sample LBA")
    args = parser.parse_args()

    sample_experiment = SampleExperiment(args.machineclass, args.samplerate, args.randomseed, args.bitmask)
    sample_experiment.run()