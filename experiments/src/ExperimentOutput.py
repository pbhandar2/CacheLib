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
        print("In experiment set")
        print(self.exp_list)
        for exp in self.exp_list:
            print(exp._output_path)
    


class ExperimentOutput:
    def __init__(self, experiment_output_path):
        self._output_path = pathlib.Path(experiment_output_path)
        self._iteration_count = int(self._output_path.stem.split("_")[-1])

        # overall stats
        self.stat = {}

        self.bandiwidth_key = "bandwidth_byte/s"

        # cache parameters 
        self.nvm_cache_size_mb = 0 
        self.ram_cache_size_mb = 0 
        self.ram_alloc_size_byte = 0
        self.page_size_byte = 0 
        self.input_queue_size = 0
        self.processor_thread_count = 0 
        self.iat_scale_factor = 0 
        self.t2_hit_start = -1
        self.tag = "unknown"

        # flag indicating whether the output is complete 
        self.full_output = False 

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
                    # these are configuration parameters stored as JSON string in the output file 
                    if "nvmCacheSizeMB" in line:
                        split_line = line.split(":")
                        self.nvm_cache_size_mb = int(split_line[1].replace(",", ""))
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
                        self.input_queue_size = int(split_line[1].replace(",", ""))
                        self.stat["inputQueueSize"] = self.input_queue_size

                    if "processorThreadCount" in line:
                        split_line = line.split(":")
                        self.processor_thread_count = int(split_line[1].replace(",", ""))
                        self.stat["processorThreadCount"] = self.processor_thread_count

                    if "scaleIAT" in line:
                        split_line = line.split(":")
                        self.iat_scale_factor = int(split_line[1].replace(",", ""))
                        self.stat["scaleIAT"] = self.iat_scale_factor 
                    
                    if "tag" in line:
                        split_line = line.split(":")
                        self.tag = split_line[1].replace(",", "")

                line = f.readline()
            
            if self.input_queue_size == 0 or self.iat_scale_factor == 0 or self.processor_thread_count == 0:
                raise ValueError("Some cache parameter missing from file {}".format(self._output_path))

        if self.is_output_complete():
            self.full_output = True


    def get_read_io_processed(self):
        return self.ts_stat[max(self.ts_stat.keys())]["readIOProcessed"]


    def get_write_io_processed(self):
        return self.ts_stat[max(self.ts_stat.keys())]["writeIOProcessed"]


    def get_bandwidth(self):
        if self.bandwidth_key not in self.stat:
            raise ValueError("No key {} in stat".format(self.bandwidth_key))
        return self.stat[self.bandwidth_key]

    
    def get_runtime(self):
        return self.stat["experimentTime_s"]


    def metric_check(self, metric_name):
        return metric_name in self.stat


    def get_metric(self, metric_name):
        if not self.metric_check(metric_name):
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