import argparse
import pathlib 
import math 
import pandas as pd 
import numpy as np 

MIN_T1_SIZE=100.0 # 100 MB
MAX_T1_SIZE=50000.0 # 50GB
MIN_T2_SIZE=150.0 # 150MB
MAX_T2_SIZE=100000.0 # 100GB
PAGE_SIZE=4096

def generate_t1_t2_file(rd_hist_file_path, t1_hr_array, t2_hr_array, output_path):
    t1_t2_array = []
    rd_hist_array = np.genfromtxt(rd_hist_file_path, delimiter=",", dtype=int)
    cum_read_hit_count = rd_hist_array[1:,0].cumsum()
    read_hit_rate = 100*cum_read_hit_count/cum_read_hit_count[-1]
    for t1_hr in t1_hr_array:
        t1_size = math.ceil(np.argmax(read_hit_rate>=t1_hr)*PAGE_SIZE/1e6)
        # ST config 
        if t1_size >= MIN_T1_SIZE and t1_size <= MAX_T1_SIZE:
            t1_t2_array.append("{},0".format(t1_size))
            for t2_hr in t2_hr_array:
                if t1_hr + t2_hr <= 100:
                    t2_size = math.ceil((np.argmax(read_hit_rate>=t1_hr+t2_hr) - t1_size)*PAGE_SIZE/1e6)
                    if t2_size >= MIN_T2_SIZE and t2_size <= MAX_T2_SIZE:
                        t1_t2_array.append("{},{}".format(t1_size, t2_size))
    with open(output_path, "w+") as f:
        f.write("\n".join(t1_t2_array))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate T1, T2 sizes ")

    parser.add_argument("rd_hist_file_path",
                            type=pathlib.Path,
                            help="The path to the hit hist file")

    parser.add_argument("output_path",
                            type=pathlib.Path,
                            help="The output file path containing tier 1, tier 2 sizes in CSV")

    parser.add_argument("--t1_hit_rate", 
                            type=float,
                            nargs='*')

    parser.add_argument("--t2_hit_rate", 
                            type=float,
                            nargs='*')

    args = parser.parse_args()

    generate_t1_t2_file(args.rd_hist_file_path, args.t1_hit_rate, args.t2_hit_rate, args.output_path)
