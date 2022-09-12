import argparse 
import pathlib 
import os
import math 
import pandas as pd 


# user defined variables 
QUEUE_SIZE_VALUES = [32, 64, 128, 256]
THREAD_COUNT_VALUES = [8, 16, 32]
IAT_SCALE_FACTOR_VALUES = [1, 10, 100, 1000, 5000]
ITERATION_COUNT_VALUES = [3,5,10]
SIZE_MULTIPLIER_VALUES = [2, 4, 8, 16]

DEFAULT_QUEUE_SIZE = 128
DEFAULT_THREAD_COUNT = 16
DEFAULT_IAT_SCALE_FACTOR = 100 
DEFAULT_ITERATION_COUNT = 3
DEFAULT_MAX_T1_SIZE_MB = 2000 
DEFAULT_MIN_T1_SIZE_MB = 100 
DEFAULT_MEMORY_LIMIT = 40000
DEFAULT_NVM_LIMIT = 400000
DEFAULT_STEP_SIZE_MB = 1000
DEFAULT_SIZE_MULTIPLIER = 16
DEFAULT_DISK_FILE_PATH = pathlib.Path.home().joinpath("disk", "disk.file")
DEFAULT_NVM_FILE_PATH = pathlib.Path.home().joinpath("nvm", "disk.file")
DEFAULT_AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
DEFAULT_AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
DEFAULT_RUN_COMMAND = "../../opt/cachelib/bin/cachebench"
DEFAULT_EXTERNAL_DATA = pathlib.Path("../data/cp_block.csv")


class Runner:
    def __init__(self, args):
        self.machine_id = args.machineID
        self.workload_id = args.workloadID
        self.step_size_mb = args.stepSizeMB
        self.size_multiplier = args.sizeMultiplier
        self.args = args 

        self.external_df = None
        if args.externalData.is_file():
            self.external_df = pd.read_csv(args.externalData)
            assert self.workload_id in self.external_df["workload"].to_list()
            self.row = self.external_df[self.external_df["workload"]==self.workload_id]
            self.max_t1_size_gb = int(math.ceil(self.row["page_working_set_size"].item()/1e9))

    
    def fixed_step(self):
        # start iteration from larger size caches that finishes faster 
        for tier1_size_mb in reversed(range(self.step_size_mb, self.max_t1_size_gb*1000, self.step_size_mb)):
            # generate all possible T2 sizes based on current T1 size and max T1 size 
            diff_from_max_tier1_size_mb = self.max_t1_size_gb*1000 - tier1_size_mb

            tier2_step_size = self.step_size_mb * self.size_multiplier
            max_tier2_size = diff_from_max_tier1_size_mb * self.size_multiplier

            for tier2_size_mb in reversed(range(tier2_step_size, max_tier2_size, tier2_step_size)):
                print(tier1_size_mb, tier2_size_mb)

                # create a config file 

                # upload the config file if it doesn't exist 





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
    
    args = parser.parse_args()

    runner = Runner(args)
    runner.fixed_step()