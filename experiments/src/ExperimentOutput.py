from multiprocessing.sharedctypes import Value
import pathlib, sys 
import numpy as np 

""" The class reads an output dump from an experiment. """

class ExperimentSet:
    def __init__(self, exp_list):
        self.exp_list = [ExperimentOutput(_) for _ in exp_list]

    def get_mean_metric(self, metric_name):
        metric_list = []
        for exp_output in self.exp_list:
            metric_value = exp_output.get_metric(metric_name)
            metric_list.append(metric_value)
        return np.mean(metric_list)

    def print_output_file_list(self):
        for exp in self.exp_list:
            print(exp._output_path)


class ExperimentOutput:
    def __init__(self, experiment_output_path):
        self.bandiwidth_key = "bandwidth_byte/s"
        self._output_path = pathlib.Path(experiment_output_path)
        
        # overall stats
        self.stat = {}

        # cache params
        self.nvm_cache_size_mb = 0 
        self.ram_cache_size_mb = 0 
        self.ram_alloc_size_byte = 0
        self.page_size_byte = 0 

        # app params
        self.queue_size = 0
        self.thread_count = 0 

        # inter-arrival time acceleration 
        self.iat_scale = 1.0 

        # tag to map output files to machines 
        self.tag = "unknown"

        # iteration count 
        self.it = int(self._output_path.stem.split("_")[-1])

        self.error = False 

        # read the file and load metrics 
        self._load()


    def _load(self):
        # load the experiment output to the class 
        with open(self._output_path) as f:
            line = f.readline()
            while line:
                line = line.rstrip()
                split_line = line.split("=")
                # a JSON string cannot have the '=' character without it being in a string with a quote '"'
                # in case there is a JSON property with an '=' in it 
                if len(split_line) == 2 and '"' not in line:
                    # these are performance metrics from CacheBench with format (*metric_name*=*metric_value*)
                    metric_name = split_line[0]
                    self.stat[metric_name] = float(split_line[1]) 
                elif 'stat:' in line:
                    pass 
                else:
                    try:
                        # these are configuration parameters stored as JSON string in the output file 
                        if "nvmCacheSizeMB" in line:
                            split_line = line.split(":")
                            self.nvm_cache_size_mb = int(split_line[1].replace(",", "").replace('"', ""))
                            self.stat["nvmCacheSizeMB"] = self.nvm_cache_size_mb

                        if "cacheSizeMB" in line:
                            split_line = line.split(":")
                            self.ram_cache_size_mb = int(split_line[1].replace(",", ""))
                            self.stat["cacheSizeMB"] = self.ram_cache_size_mb

                        if "allocSizes" in line:
                            self.ram_alloc_size_byte = int(f.readline().rstrip())
                            self.stat["t1AllocSize"] = self.ram_alloc_size_byte

                        if "pageSizeBytes" in line:
                            split_line = line.split(":")
                            self.page_size_byte = int(split_line[1].replace(",", ""))
                            self.stat["pageSizeBytes"] = self.page_size_byte

                        if "inputQueueSize" in line:
                            split_line = line.split(":")
                            self.queue_size = int(split_line[1].replace(",", ""))
                            self.stat["inputQueueSize"] = self.queue_size

                        if "processorThreadCount" in line:
                            split_line = line.split(":")
                            self.thread_count = int(split_line[1].replace(",", ""))
                            self.stat["processorThreadCount"] = self.thread_count

                        if "scaleIAT" in line:
                            split_line = line.split(":")
                            self.iat_scale = int(split_line[1].replace(",", ""))
                            self.stat["scaleIAT"] = self.iat_scale 
                        
                        if "tag" in line:
                            split_line = line.split(":")
                            self.tag = split_line[1].replace(",", "").replace('"', '')
                    except:
                        self.error = True 

                line = f.readline()
            
            if self.queue_size == 0 or self.iat_scale == 0 or self.thread_count == 0:
                raise ValueError("Some cache parameter missing from file {}".format(self._output_path))


    def get_bandwidth(self):
        if self.bandwidth_key not in self.stat:
            raise ValueError("No key {} in stat".format(self.bandwidth_key))
        return self.stat[self.bandwidth_key]

    
    def get_runtime(self):
        return self.stat["experimentTime_s"]


    def get_metric(self, metric_name):
        if metric_name not in self.stat:
            return np.inf 
        return self.stat[metric_name]


    def is_output_complete(self):
        return "t2WriteLat_p100_us" in self.stat and \
                "t2GetCount" in self.stat and "inputQueueSize" in self.stat and \
                "bandwidth_byte/s" in self.stat 


    def get_block_req_per_second(self):
        return self.stat["blockReqCount"]/self.stat["experimentTime_s"]


    def get_t2_size_mb(self):
        return self.nvm_cache_size_mb

    def get_error(self):
        return self.error 


    def get_t1_size_mb(self):
        return self.ram_cache_size_mb


    def get_t1_hit_rate(self):
        return self.stat["t1HitRate"]


    def get_t2_hit_rate(self):
        return self.stat["t2HitRate"]


    def get_percentile_read_slat(self, percentile_str):
        return self.stat["blockReadSlat_{}_ns".format(percentile_str)]


    def get_percentile_write_slat(self, percentile_str):
        return self.stat["blockWriteSlat_{}_ns".format(percentile_str)]


    def get_row(self):
        return self.stat 


    def get_exp_config(self):
        exp_config = {
            "queue_size": self.queue_size,
            "thread_count": self.thread_count,
            "iat_scale": self.iat_scale,
            "t1_size_mb": self.ram_cache_size_mb,
            "t2_size_mb": self.nvm_cache_size_mb,
            "it": self.it,
            "tag": self.tag 
        }
        return exp_config


    def __str__(self):
        repr_str = "ExperimentOutput:Size[T1/T2]={},{}, \
                    HR(%)[T1/T2]={:3.1f},{:3.1f}, \
                    MEANSLAT(ms)[R/W]={:3.2f},{:3.2f},{:3.2f},{:3.2f}, \
                    BANDWIDTH={:3.2f}".format(self.get_ram_size(), 
                                                self.get_nvm_size(), 
                                                self.get_t1_hit_rate(), 
                                                self.get_t2_hit_rate(),
                                                self.get_percentile_read_slat("avg")/1e6,
                                                self.get_percentile_write_slat("avg")/1e6,
                                                self.get_percentile_read_slat("p99")/1e6,
                                                self.get_percentile_write_slat("p99")/1e6,
                                                self.get_bandwidth()/1e6)
        return repr_str