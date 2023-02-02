import pathlib 
import argparse 
import json 
import pandas as pd 
from itertools import product

from Experiment import Experiment 
from Runner import Runner 


class RunSampleSet(Experiment):
    def __init__(self, machine):
        super().__init__(machine)
        self.sample_rate_list = [0, 0.001, 0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8]
        self.random_seed_list = [42]
        self.num_bit_shift = [0, 1, 2, 4, 8, 16, 32]
        self.experiment_list_file = pathlib.Path('./data/RunSampleSet/experiment_list.csv')
        self.experiment_df = pd.read_csv(self.experiment_list_file)
    

    def check_max_sample_rate_is_eligible(self, t1_size, t2_size):
        max_sample_rate = max(self.sample_rate_list)
        sample_t1_size = int(max_sample_rate * t1_size)
        sample_t2_size = int(max_sample_rate * t2_size)
        return self.tier_size_eligible(sample_t1_size, sample_t2_size)
    

    def run_sample(self, experiment_row, sample_rate, random_seed, num_bit_shift):
        sample_name = "sample_basic"
        t1_size, t2_size = experiment_row['t1_size'], experiment_row['t2_size']
        sample_t1_size, sample_t2_size = int(sample_rate*t1_size), int(sample_rate*t2_size)
        s3_key_prefix = "experiment_output_dump/{}".format(sample_name)

        print(t1_size, t2_size, sample_t1_size, sample_t2_size, sample_rate, random_seed, num_bit_shift)

        # run default experiment since sampling is disabled 
        if not self.tier_size_eligible(sample_t1_size, sample_t2_size):
            print("Tier sizes not eligigble {}, {}".format(sample_t1_size, sample_t2_size))
            return -1 

        # generate config
        config = self.get_base_config(experiment_row['queue_size'], experiment_row['thread_count'], experiment_row['replay_rate'], sample_t1_size, sample_t2_size)
        with open(self.machine_details["cachebench_config_path"], "w+") as f:
            f.write(json.dumps(config, indent=4))

        for it in range(self.it_count):
            s3_key_suffix = "{}/{}_{}_{}_{}_{}_{}_{}_{}_{}".format(experiment_row['workload'],
                                                            experiment_row['queue_size'],
                                                            experiment_row['thread_count'], 
                                                            experiment_row['replay_rate'],
                                                            t1_size,
                                                            t2_size,
                                                            sample_rate, 
                                                            random_seed, 
                                                            num_bit_shift,
                                                            it)
            
            key_dict = self.get_key_dict(s3_key_prefix, s3_key_suffix)

            # check if the new key already exists 
            if self.check_experiment_status(key_dict):
                print("experiment already done {}, {}, {}".format(key_dict['done'], self.s3.get_key_size(key_dict['done']), self.s3.get_key_size(key_dict['live'])))
                return key_dict, -1

            # lock the experiment 
            self.s3.upload_s3_obj(key_dict['live'], self.machine_details["cachebench_config_path"])

            if it == 0:
                # download the block trace 
                block_trace_key = "sample_block_trace/{}/{}/{}_{}_{}.csv".format(sample_name, experiment_row['workload'], random_seed, sample_rate, num_bit_shift)
                if not self.s3.get_key_size(block_trace_key):
                    print("Couldn't find block trace {}".format(block_trace_key))
                    return key_dict, -1
                self.s3.download_s3_obj(block_trace_key, self.machine_details["block_trace_path"])

            print(config)
            runner = Runner(self.machine_details["exp_output_path"], self.machine_details["usage_output_path"])
            return_code = runner.run(self.machine_details["cachebench_config_path"])
            if return_code > 0:
                self.s3.upload_s3_obj(key_dict['error'], str(self.machine_details["exp_output_path"]))
                self.s3.delete_s3_obj(key_dict['live'])
                return 
            elif return_code == 0:
                self.s3.upload_s3_obj(key_dict['done'], str(self.machine_details["exp_output_path"]))
                self.s3.upload_s3_obj(key_dict['usage'], str(self.machine_details["usage_output_path"]))
                self.s3.delete_s3_obj(key_dict['live'])
                print("Sucessfully ran s3 key: {}".format(key_dict['done']))
    

    def run_default(self, experiment_row):
        t1_size, t2_size = experiment_row['t1_size'], experiment_row['t2_size']
        s3_key_prefix = "experiment_output_dump/default"

        # run default experiment since sampling is disabled 
        if not self.tier_size_eligible(t1_size, t2_size):
            print("Tier sizes not eligigble {}, {}".format(t1_size, t2_size))
            return -1 

        # generate config
        config = self.get_base_config(experiment_row['queue_size'], experiment_row['thread_count'], experiment_row['replay_rate'], t1_size, t2_size)
        with open(self.machine_details["cachebench_config_path"], "w+") as f:
            f.write(json.dumps(config, indent=4))

        for it in range(self.it_count):
            s3_key_suffix = "{}/{}_{}_{}_{}_{}_{}".format(experiment_row['workload'],
                                                            experiment_row['queue_size'],
                                                            experiment_row['thread_count'], 
                                                            experiment_row['replay_rate'],
                                                            t1_size,
                                                            t2_size,
                                                            it)
            
            key_dict = self.get_key_dict(s3_key_prefix, s3_key_suffix)

            # check if the new key already exists 
            if self.check_experiment_status(key_dict):
                return key_dict, -1

            # lock the experiment 
            self.s3.upload_s3_obj(key_dict['live'], self.machine_details["cachebench_config_path"])

            if it == 0:
                # download the block trace 
                block_trace_key = "block_trace/{}.csv".format(experiment_row['workload'])
                if not self.s3.get_key_size(block_trace_key):
                    print("Couldn't find block trace {}".format(block_trace_key))
                    return key_dict, -1
                self.s3.download_s3_obj(block_trace_key, self.machine_details["block_trace_path"])

            print(config)
            runner = Runner(self.machine_details["exp_output_path"], self.machine_details["usage_output_path"])
            return_code = runner.run(self.machine_details["cachebench_config_path"])
            if return_code > 0:
                self.s3.upload_s3_obj(key_dict['error'], str(self.machine_details["exp_output_path"]))
                self.s3.delete_s3_obj(key_dict['live'])
                return 
            elif return_code == 0:
                self.s3.upload_s3_obj(key_dict['done'], str(self.machine_details["exp_output_path"]))
                self.s3.upload_s3_obj(key_dict['usage'], str(self.machine_details["usage_output_path"]))
                self.s3.delete_s3_obj(key_dict['live'])
                print("Sucessfully ran s3 key: {},{}".format(key_dict['done']))


    def run_set(self, experiment_row):
        param_combo = product(self.sample_rate_list, self.random_seed_list, self.num_bit_shift)
        for sample_rate, random_seed, num_bit_shift in param_combo:
            if sample_rate == 0:
                self.run_default(experiment_row)
            else:
                self.run_sample(experiment_row, sample_rate, random_seed, num_bit_shift)

                
    def run(self, args):
        for row_index, experiment_row in self.experiment_df.iterrows():
            t1_size, t2_size = experiment_row['t1_size'], experiment_row['t2_size']
            if not self.check_max_sample_rate_is_eligible(t1_size, t2_size):
                continue 
            
            self.run_set(experiment_row)

            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run trace replay for 3 iterations with specified parameters")
    parser.add_argument("machineclass", 
                            type=str, 
                            help="The class of the machine")
    args = parser.parse_args()

    runner = RunSampleSet(args.machineclass)
    runner.run(args)