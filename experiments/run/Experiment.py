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
                    min_t1_size = 100, 
                    size_multiplier = [0,2,4,8], 
                    global_config_file = "global_config.json"):
        
        # load the global configuration 
        with open(global_config_file) as f:
            self.global_config = json.load(f)

        # s3 client 
        self.s3 = S3Client()
        
        # options arguments 
        self.min_t1_size_mb = min_t1_size
        self.size_multiplier = size_multiplier

        # arguments 
        self.experiment_id = experiment_id
        self.machine_id = machine_id
        self.step_size_mb = step_size_mb
        self.max_tier1_size_mb = max_tier1_size_mb
        self.aws_access_key = aws_access_key 
        self.aws_secret_key = aws_secret_key
        

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


    def get_output(self, 
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


    def default_generator(self, workload_id, output_file_path):
        experiment_config = self.global_config
        
        queue_size = experiment_config["default"]["queue_size"]
        thread_count = experiment_config["default"]["thread_count"]
        iat_scale_factor = experiment_config["default"]["iat_scale_factor"]
        min_iteration = experiment_config["default"]["min_iteration"]

        out_handle = open(output_file_path, "w+")

        # Max tier-1 cache size ST case 
        for cur_iteration in range(min_iteration):
            self.get_output(self.experiment_id,
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
            for tier2_size_multiplier in self.size_multiplier:
                t1_size_reduced_mb = self.max_tier1_size_mb - tier1_size_mb
                tier2_size_mb = t1_size_reduced_mb * tier2_size_multiplier

                for cur_iteration in range(min_iteration):
                    self.get_output(self.experiment_id,
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
    disk_and_nvm_file_check(args.d, args.n)
    experiment = Experiment(args.experiment_id, 
                                args.machine_id, 
                                args.s, 
                                args.m, 
                                args.awskey, 
                                args.awssecret)
    experiment.default_generator(args.workload_id, args.o)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION)
    parser.add_argument("experiment_id",
                            help="Experiment identification")
    parser.add_argument("machine_id",
                            help="Machine identification")
    parser.add_argument("workload_id",
                            help="Workload identification")
    parser.add_argument("--awskey",
                            help="AWS access key")
    parser.add_argument("--awssecret",
                            help="AWS secret key")
    parser.add_argument("--o",
                            default=pathlib.Path("out.csv"),
                            type=pathlib.Path,
                            help="Output path of the experiment list")
    parser.add_argument("--d",
                            default=pathlib.Path.home().joinpath("disk", "disk.file"),
                            type=pathlib.Path,
                            help="Path to file on disk")
    parser.add_argument("--n",
                            default=pathlib.Path.home().joinpath("nvm", "disk.file"),
                            type=pathlib.Path,
                            help="Path to file on NVM")
    parser.add_argument("--s",
                            default=200,
                            type=int,
                            help="Step size in MB")
    parser.add_argument("--m",
                            default=1000,
                            type=int,
                            help="Max tier-1 cache size")
    main(parser.parse_args())