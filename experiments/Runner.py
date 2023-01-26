""" This class runs a CacheBench experiment with a given configuration 
    file. The output of the experiment and the resource (memory,CPU)
    usage of the experiment are logged to files specified by the user. 
"""

import pathlib 
import subprocess
import psutil 
import time 
import sys 


class Runner:
    def __init__(self, experiment_output_path, resource_usage_output_path):
        self.usage_snapshot_window = 300 
        self.run_cachebench_str = "../opt/cachelib/bin/cachebench"
        self.experiment_output_path = pathlib.Path(experiment_output_path)
        self.resource_usage_output_path = pathlib.Path(resource_usage_output_path)


    def snap_system_stats(self, usage_handle):
        """ Write memory and CPU stats to a file. 

            Parameters
            ----------
            usage_handle : int 
                the handle of the file where memory and CPU usage are logged 
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

    
    def run(self, cachebench_config_file_path):
        """ Run the CacheBench experiment and track its memory and CPU 
            usage. 

            Parameters
            ----------
            cachebench_config_file_path : pathlib.Path
                path to JSON file containing CacheBench configuration 
            
            Return 
            ------
            return_code : int 
                the status of experiment process on termination 
        """

        return_code = -1 
        start_time = time.time()
        with self.experiment_output_path.open("w+") as output_handle, \
                self.resource_usage_output_path.open("w+") as usage_handle:

            # run the cachelib experiment 
            with subprocess.Popen([self.run_cachebench_str, 
                                    "--json_test_config", 
                                    str(cachebench_config_file_path)], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.STDOUT, 
                                    bufsize=1,
                                    encoding='utf-8') as process_handle:
                
                # write every line to output dump 
                for line in process_handle.stdout: # b'\n'-separated lines
                    sys.stdout.buffer.write(bytes(line, 'utf-8')) 
                    output_handle.write(line)
                    cur_time = time.time()
                    if (cur_time - start_time) > self.usage_snapshot_window:
                        self.snap_system_stats(usage_handle)
                        start_time = cur_time 

                # wait for it to complete 
                process_handle.wait()
                return_code = process_handle.returncode
        
        return return_code