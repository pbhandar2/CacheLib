import argparse
import pathlib 
import math 
import json 
import pandas as pd 
import numpy as np 


def generate_config(
        queue_size, 
        thread_count, 
        iat_scale_factor, 
        block_trace_path,
        t1_size,
        t2_size,
        output_path):

    with open("global_config.json") as f:
        global_config = json.load(f)

    config = {}
    if t2_size == 0:
        with open("st_config_template.json") as f:
            config = json.load(f)
        config["cache_config"]["cacheSizeMB"] = t1_size 
    else:
        with open("mt_config_template.json") as f:
            config = json.load(f)
        config["cache_config"]["cacheSizeMB"] = t1_size 
        config["cache_config"]["nvmCacheSizeMB"] = t2_size
        config["cache_config"]["nvmCachePaths"] = str(pathlib.Path(global_config["nvm_file_path"]).expanduser())
    
    config["test_config"]["diskFilePath"] = str(pathlib.Path(global_config["disk_file_path"]).expanduser())
    config["test_config"]["maxDiskFileOffset"] = pathlib.Path(global_config["disk_file_path"]).expanduser().stat().st_size
    config["test_config"]["inputQueueSize"] = queue_size
    config["test_config"]["processorThreadCount"] = thread_count
    config["test_config"]["asyncIOTrackerThreadCount"] = thread_count
    config["test_config"]["scaleIAT"] = iat_scale_factor
    config["test_config"]["replayGeneratorConfig"]["traceList"] = [str(block_trace_path.absolute())]

    with open(output_path, "w+") as out_handle:
        out_handle.write(json.dumps(config, indent=4))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate config file")

    parser.add_argument("queue_size",
                            type=int,
                            help="Max pending block requests")

    parser.add_argument("thread_count",
                            type=int,
                            help="Number of threads each for processing block requests and completed async IO")

    parser.add_argument("iat_scale_factor", 
                            type=int,
                            help="The factor by which the IAT(Inter Arrival Time) is divided")

    parser.add_argument("block_trace_path", 
                            type=pathlib.Path,
                            help="Path to block trace")

    parser.add_argument("t1_size", 
                            type=int,
                            help="Size of tier-1 cache")

    parser.add_argument("t2_size", 
                            type=int,
                            help="Size of tier-2 cache")

    parser.add_argument("output_path", 
                            type=pathlib.Path,
                            help="Output path of config")

    args = parser.parse_args()

    generate_config(
        args.queue_size, 
        args.thread_count, 
        args.iat_scale_factor, 
        args.block_trace_path,
        args.t1_size, 
        args.t2_size,
        args.output_path)
