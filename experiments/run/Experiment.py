import json 
import pathlib 
import argparse 
from S3Client import S3Client

SCRIPT_DESCRIPTION = """ Generate experiments to be run based on user input. """

class Experiment:
    def __init__(self, 
                    experiment_id, 
                    machine_id, 
                    step_size_mb, 
                    max_tier1_size_mb, 
                    aws_access_key,
                    aws_secret_key,
                    size_multiplier, 
                    min_t1_size = 100, 
                    global_config_file = "global_config.json"):
        
        # load the global configuration 
        with open(global_config_file) as f:
            self.global_config = json.load(f)

        # s3 client 
        self.s3 = S3Client()
        
        # options 
        self.min_t1_size_mb = min_t1_size

        # arguments 
        self.experiment_id = experiment_id
        self.machine_id = machine_id
        self.step_size_mb = step_size_mb
        self.max_tier1_size_mb = max_tier1_size_mb
        self.aws_access_key = aws_access_key 
        self.aws_secret_key = aws_secret_key
        self.size_multiplier = size_multiplier
        

    def check_if_output_key_exists(self, key):
        return self.s3.check(key) > 0


    def get_s3_key(self, 
                    experiment_id, 
                    machine_id, 
                    workload_id,
                    queue_size, 
                    thread_count, 
                    iat_scale_factor,  
                    t1_size, 
                    t2_size,
                    iteration):
        return "output/{}/{}/{}/{}_{}_{}_{}_{}_{}".format(experiment_id,
                                                            machine_id,
                                                            workload_id,
                                                            queue_size,
                                                            thread_count,
                                                            iat_scale_factor,
                                                            t1_size,
                                                            t2_size,
                                                            iteration)


    def write_experiment_to_file(self, 
                                    experiment_id, 
                                    machine_id, 
                                    workload_id,
                                    queue_size, 
                                    thread_count, 
                                    iat_scale_factor,  
                                    tier1_size_mb, 
                                    tier2_size_mb,
                                    iteration,
                                    out_handle):
        # key of the experiment 
        key = self.get_s3_key(self.experiment_id,
                                self.machine_id,
                                workload_id,
                                queue_size,
                                thread_count,
                                iat_scale_factor,
                                tier1_size_mb,
                                tier2_size_mb,
                                iteration)

        out_str = None
        # check if cache size requirements are satisfied 
        if (tier1_size_mb > self.global_config["max_t1_size_mb"] or \
                tier2_size_mb > self.global_config["max_t2_size_mb"]):
            print("Cache size limit crossed. Limit-> T1:{},T2:{}, Size-> T1:{},T2:{}".format(self.global_config["max_t1_size_mb"],
                                                                                                self.global_config["max_t2_size_mb"],
                                                                                                tier1_size_mb,                                                                              tier2_size_mb))
            return out_str
        
        # check if key exists in S3 bucket already 
        if (self.s3.check(key) == 0):
            out_str = "{},{},{},{},{},{},{},{},{},{}\n".format(experiment_id,
                                                                machine_id,
                                                                workload_id,
                                                                queue_size,
                                                                thread_count,
                                                                iat_scale_factor,
                                                                tier1_size_mb,
                                                                tier2_size_mb,
                                                                iteration,
                                                                key)
            print(out_str)
            out_handle.write(out_str)
        else:
            print("Key {} already exists!".format(key))

        return out_str 


    def basic(self, 
                workload_id, 
                queue_size,
                thread_count,
                iat_scale_factor,
                min_iteration,
                output_file_path):

        out_handle = open(output_file_path, "w+")
        
        # Max tier-1 cache size ST case 
        for cur_iteration in range(min_iteration):
            self.write_experiment_to_file(self.experiment_id,
                                            self.machine_id,
                                            workload_id,
                                            queue_size,
                                            thread_count,
                                            iat_scale_factor,
                                            self.max_tier1_size_mb,
                                            0,
                                            cur_iteration,
                                            out_handle)

        for tier1_size_mb in reversed(range(max(self.min_t1_size_mb, self.step_size_mb), 
                        self.max_tier1_size_mb, 
                        self.step_size_mb)):

                # based on how much tier-1 is reduced from max tier-1 size 
                # determine the tier-2 size based on the size multiplier 
                t1_size_reduced_mb = self.max_tier1_size_mb - tier1_size_mb
                tier2_size_mb = t1_size_reduced_mb * self.size_multiplier

                for cur_iteration in range(min_iteration):
                    self.write_experiment_to_file(self.experiment_id,
                                                    self.machine_id,
                                                    workload_id,
                                                    queue_size,
                                                    thread_count,
                                                    iat_scale_factor,
                                                    tier1_size_mb,
                                                    tier2_size_mb,
                                                    cur_iteration,
                                                    out_handle)
        out_handle.close()


def disk_and_nvm_file_check(disk_file_path, nvm_file_path):
    assert(disk_file_path.exists())
    assert(nvm_file_path.exists())


def main(args):
    disk_and_nvm_file_check(args.diskFilePath, args.nvmFilePath)
    print("Running experiment id: {}".format(args.experimentID))
    experiment = Experiment(args.experimentID, 
                                args.machineID, 
                                args.stepSizeMB, 
                                args.maxRAMCacheSize, 
                                args.awsKey, 
                                args.awsSecret,
                                args.sizeMultiplier)
    
    experiment.basic(args.workloadID, 
                        args.queueSize,
                        args.threadCount,
                        args.iatScaleFactor,
                        args.iterationCount,
                        args.outputPath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION)

    parser.add_argument("experimentID",
                            help="Experiment identification")

    parser.add_argument("machineID",
                            help="Machine identification")

    parser.add_argument("workloadID",
                            help="Workload identification")

    parser.add_argument("--queueSize",
                            type=int,
                            default=128,
                            help="Maximum outstanding requests queued in the system")

    parser.add_argument("--threadCount",
                            type=int,
                            default=16,
                            help="Number of threads used for each of block request and async IO processing")
        
    parser.add_argument("--iatScaleFactor",
                            type=int,
                            default=100,
                            help="The value by which to divide IAT between block requests")

    parser.add_argument("--iterationCount",
                            type=int,
                            default=3,
                            help="Number of iterations to run per configuration")

    parser.add_argument("--awsKey",
                            help="AWS access key")
    
    parser.add_argument("--awsSecret",
                            help="AWS secret key")

    parser.add_argument("--outputPath",
                            default=pathlib.Path("~").expanduser().joinpath("out.csv"),
                            type=pathlib.Path,
                            help="Output path of the experiment list")
    
    parser.add_argument("--diskFilePath",
                            default=pathlib.Path.home().joinpath("disk", "disk.file"),
                            type=pathlib.Path,
                            help="Path to file on disk")
    
    parser.add_argument("--nvmFilePath",
                            default=pathlib.Path.home().joinpath("nvm", "disk.file"),
                            type=pathlib.Path,
                            help="Path to file on NVM")
    
    parser.add_argument("--maxRAMCacheSize",
                            default=1000,
                            type=int,
                            help="Max tier-1 cache size")

    parser.add_argument("--stepSizeMB",
                            type=int,
                            default=200,
                            help="Step size in MB")

    parser.add_argument("--sizeMultiplier",
                            type=int,
                            default=8,
                            help="Size multiplier used to determine the size of tier-2 cache")
    
    main(parser.parse_args())