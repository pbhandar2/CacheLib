import socket 
import json 
import pathlib 
from abc import ABC 

from S3Client import S3Client


class Experiment(ABC):
    def __init__(self, machine):
        self.machine = machine 
        self.s3 = S3Client()
        self.tag = socket.gethostname()
        with open("./src/data/base_experiment_params.json") as f:
            self.exp_params = json.load(f)
            self.machine_details = self.exp_params["machine"][machine]
        self.it_count = 3
        self.cachelib_min_t1_size_mb = 100 
        self.cachelib_min_t2_size_mb = 150


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


    def get_base_config(self, queue_size, thread_count, replay_rate, t1_size, t2_size):
        """ Get the dict of basic experiment configuration that we need for all experiments. 
            Specific experiments can add additional parameters later. 

            Parameters
            ----------
            queue_size : int 
                the application queue size 
            thread_count : int 
                the number of threads used to process storage IO request to the system and async IO to backing store 
            replay_rate : int 
                the replay rate (1 means same as original)
            t1_size : int 
                the size of tier-1 cache in MB 
            t2_size : int 
                the size of tier-2 cache in MB 
            
            Return 
            ------
            config : dict 
                the dict with basic experiment configuration needed to sucessfully run an experiment 
        """
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

    def create_cachebench_config_file(self, queue_size, thread_count, replay_rate, t1_size, t2_size):
        cachebench_config = self.get_base_config(queue_size, 
                                                    thread_count,
                                                    replay_rate,
                                                    t1_size, 
                                                    t2_size)

        with open(self.machine_details["cachebench_config_path"], "w+") as f:
            f.write(json.dumps(cachebench_config, indent=4))

    def tier_size_eligible(self, t1_size, t2_size):
        """ Checks if the tier sizes are eligible based on the limits
            of CacheLib and the machine. 

            Parameters
            ----------
            t1_size : int 
                tier-1 size in MB 
            t2_size : int 
                tier-2 size in MB 

            Return 
            ------
            eligibility : bool 
                a boolean that represents whether the tier sizes can be run 
        """
        return (t1_size>=self.cachelib_min_t1_size_mb and (t2_size==0 or t2_size>=self.cachelib_min_t2_size_mb) and \
                t1_size <= (self.machine_details["max_t1_size"]*1e3) and \
                t2_size <= (self.machine_details["max_t2_size"]*1e3))
    
    def get_key_dict(self, s3_key_prefix, s3_key_suffix):
        """ Get the dict of keys related to the experiments denoted by the 
            S3 suffix. 

            Parameters
            ----------
            s3_key_suffix : str 
                the s3 key suffix that represents a specific experiment 
            
            Return 
            ------
            key_dict : dict 
                dict containing keys related to the experiment represented by s3_key_suffix 
        """
        key_dict = {}
        for key_type in ["live", "done", "error", "usage"]:
            key_dict[key_type]  = "{}/{}/{}/{}".format(s3_key_prefix, key_type, self.machine, s3_key_suffix)
            
        return key_dict
    
    def download_storage_trace(self, block_trace_key):
        """ Download a block trace stored in the given key 

            Parameter
            ---------
            block_trace_key : str 
                the key where the block trace is located 
            
            Return 
            ------
            status : bool
                status that represents whether the download completed sucessfully 
        """
        if not self.s3.get_key_size(block_trace_key):
            print("Couldn't find block trace {}".format(block_trace_key))
            return False 

        try:
            self.s3.download_s3_obj(block_trace_key, self.machine_details["block_trace_path"])
            return True 
        except:
            return False 