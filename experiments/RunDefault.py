import pathlib 
import argparse 

from Experiment import Experiment 
from Runner import Runner 


class RunDefault(Experiment):
    def __init__(self, name, machine):
        super().__init__(name, machine)

    def run(self, args):
        # return immediately if tier sizes are too small or too large 
        if not self.tier_size_eligible(args.t1_size, args.t2_size):
            print("Tier sizes not eligigble {}, {}".format(args.t1_size, args.t2_size))
            return key_dict, -1
        
        for it in range(self.it_count):
            s3_key_suffix = "{}/{}_{}_{}_{}_{}_{}".format(args.workload,
                                                            args.queue_size,
                                                            args.thread_count, 
                                                            args.replay_rate,
                                                            args.t1_size,
                                                            args.t2_size,
                                                            it)
            
            key_dict = self.get_key_dict(s3_key_suffix)

            # lock the experiment 
            self.s3.upload_s3_obj(key_dict['live'], self.machine_details["cachebench_config_path"])

            print("Locked: {}".format(key_dict['live']))

            if it == 0:
                self.create_cachebench_config_file(args.queue_size, 
                                                    args.thread_count,
                                                    args.replay_rate,
                                                    args.t1_size, 
                                                    args.t2_size)
                
                storage_trace_key = "block_trace/{}.csv".format(args.workload)
                download_status = self.download_storage_trace(storage_trace_key)
                if not download_status:
                    print("Block trace {} could not be downloaded".format(storage_trace_key))
                    return key_dict, -1 

            runner = Runner(self.machine_details["exp_output_path"], self.machine_details["usage_output_path"])
            return_code = runner.run(self.machine_details["cachebench_config_path"])
            return key_dict, return_code

            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run trace replay for 3 iterations with specified parameters")
    parser.add_argument("machineclass", 
                            type=str, 
                            help="The class of the machine")
    parser.add_argument("workload", 
                            type=str, 
                            help="Workload to replay")
    parser.add_argument("t1_size",
                            type=int,
                            help="Size of tier-1 cache in MB")
    parser.add_argument("t2_size",
                            type=int,
                            help="Size of tier-2 cache in MB")
    parser.add_argument("--queue_size",
                            type=int,
                            default=128,
                            help="Application queue size")
    parser.add_argument("--thread_count",
                            type=int,
                            default=16,
                            help="The number of threads used for storage request and async IO processing")
    parser.add_argument("--replay_rate",
                            type=int,
                            default=1,
                            help="Replay rate (divide IAT of each request by this value)")
    args = parser.parse_args()

    runner = RunDefault("default", args.machineclass)
    runner.run(args)