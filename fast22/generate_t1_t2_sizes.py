import argparse
import pathlib 
import math 
import pandas as pd 
import numpy as np 
import boto3 
import logging 
from botocore.exceptions import ClientError


MIN_T1_SIZE_MB=100.0 # 100 MB
MAX_T1_SIZE_MB=50000.0 # 50GB
MIN_T2_SIZE_MB=150.0 # 150MB
MAX_T2_SIZE_MB=200000.0 # 200GB
PAGE_SIZE=4096 # 4KB 


def get_read_hit_rate(rd_hist_file_path):
    rd_hist_array = np.genfromtxt(rd_hist_file_path, delimiter=",", dtype=int)
    cum_read_hit_count = rd_hist_array[1:,0].cumsum()
    read_hit_rate = 100*cum_read_hit_count/cum_read_hit_count[-1]
    return read_hit_rate


def generate_all_possible_t1_t2_sizes(rd_hist_file_path, t1_hr_array, t2_hr_array):
    t1_t2_array = []
    read_hit_rate = get_read_hit_rate(rd_hist_file_path)
    for t1_hr in t1_hr_array:
        # min cache size MB needed to attain the specified hit rate 
        t1_size_mb = int(math.ceil(np.argmax(read_hit_rate>=t1_hr)*PAGE_SIZE/1e6))
        if t1_size_mb >= MIN_T1_SIZE_MB and t1_size_mb <= MAX_T1_SIZE_MB:
            # add the ST case to the list 
            t1_t2_array.append([t1_size_mb, 0])
            for t2_hr in t2_hr_array:
                # sum of hit rate can only be up to 100 
                if t1_hr + t2_hr <= 100:
                    # we assume an exclusive cache 
                    # cache size needed to obtain hit rate t1_hr+t2_hr represents 
                    # the sum (t1_size_mb+t2_size_mb) needed to obtain that hit rate 
                    # so to get t2_size_mb = (t1_size_mb+t2_size_mb) - t1_size_mb
                    t2_size_mb = int(math.ceil((np.argmax(read_hit_rate>=t1_hr+t2_hr) - t1_size_mb)*PAGE_SIZE/(1e6)))
                    if t2_size_mb >= MIN_T2_SIZE_MB and t2_size_mb <= MAX_T2_SIZE_MB:
                        t1_t2_array.append([t1_size_mb, t2_size_mb])
        else:
            print("Dropped tier size with tier 1 size {}".format(t1_size_mb))
    return t1_t2_array


def generate_t1_t2_file(rd_hist_file_path, t1_hr_array, t2_hr_array, output_path, partial_key, iteration):
    t1_t2_array = generate_all_possible_t1_t2_sizes(rd_hist_file_path, t1_hr_array, t2_hr_array)
    s3 = boto3.client('s3')
    with open(output_path, "w+") as f:
        for t1_t2_size in t1_t2_array:
            t1_size = t1_t2_size[0]
            t2_size = t1_t2_size[1]
            final_key = "{}_{}_{}_{}".format(partial_key, t1_size, t2_size, iteration)
            key_count = s3.list_objects_v2(Bucket='mtcachedata', Prefix=final_key)['KeyCount']
            if key_count == 0:
                f.write("{},{}\n".format(str(t1_size), str(t2_size)))
            else:
                print("Key {} already exists. Count: {}".format(final_key, key_count))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate T1, T2 sizes ")

    parser.add_argument("rd_hist_file_path",
                            type=pathlib.Path,
                            help="The path to the RD hist file")

    parser.add_argument("output_path",
                            type=pathlib.Path,
                            help="The output file path containing tier 1, tier 2 sizes in CSV")

    parser.add_argument("--t1_hit_rate", 
                            type=float,
                            nargs='*')

    parser.add_argument("--t2_hit_rate", 
                            type=float,
                            nargs='*')

    parser.add_argument("--output_key_partial", 
                            type=str,
                            default="",
                            help="Partial key")

    parser.add_argument("--iteration", 
                            type=int,
                            default=0,
                            help="Iteration")

    args = parser.parse_args()

    generate_t1_t2_file(args.rd_hist_file_path, 
                        args.t1_hit_rate, 
                        args.t2_hit_rate, 
                        args.output_path,
                        args.output_key_partial,
                        args.iteration)