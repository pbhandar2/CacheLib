from argparse import ArgumentParser 
from pathlib import Path 
from json import dump 
from os import environ 
from pandas import read_csv 
from copy import deepcopy

from cydonia.util.S3Client import S3Client

from ReplayConfig import ReplayConfig
from Runner import Runner 

SAMPLE_TYPE = "basic"
WORKLOAD_TYPE = "cp"
KEY_BASE = "mtcachedata/new-replay/output"
CUR_OUTPUT_SET = "set-0"
EXPERIMENT_FILE_PATH = "./files/experiments.csv"
EXPERIMENT_REMOTE_FILE_KEY = "mtcachedata/new-replay/experiments.csv"

def create_replay_config(args):
    traces = [str(Path(args.tracePath).absolute())]
    backing_files = [args.backingFilePath]
    t1_size_mb = args.t1Size 

    kwargs = {}
    if args.t2Size > 0:
        pass 
    
    kwargs["statOutputDir"] = str(args.statOutDir.absolute())
    replay_config = ReplayConfig(traces, backing_files, t1_size_mb, **kwargs)
    return replay_config.get_config()


def run_trace_replay(replay_config: dict, args):
    cachebench_path = str(Path.cwd().parent.joinpath("opt/cachelib/bin/cachebench").absolute())
    assert Path(cachebench_path).exists()

    config_file_path = args.statOutDir.joinpath("config.json")
    run_cmd = "{} --json_test_config {}".format(cachebench_path, str(config_file_path.absolute()))


    
    stdout_path = str(args.statOutDir.joinpath("stdout.dump").absolute())
    stderr_path = str(args.statOutDir.joinpath("stderr.dump").absolute())
    usage_path = args.statOutDir.joinpath("usage.csv")
    power_path = args.statOutDir.joinpath("power.csv")

    print(run_cmd, stdout_path, stderr_path, usage_path, power_path)

    runner = Runner()
    runner.run(run_cmd, stdout_path, stderr_path, usage_path, power_path)


def start_next_experiment(args):
    experiment_found = False 
    # download the latest experiments list file 
    s3_client = S3Client(environ["AWS_KEY"], environ["AWS_SECRET"], environ["AWS_BUCKET"])
    s3_client.download_s3_obj(EXPERIMENT_REMOTE_FILE_KEY, EXPERIMENT_FILE_PATH)

    new_args = deepcopy(args)
    exp_df = read_csv(EXPERIMENT_FILE_PATH)
    for row_index, row in exp_df.iterrows():
        t1_size, t2_size = int(row["t1"]), int(row["t2"])
        trace_name = Path(row["traceKey"]).stem 
        exp_dir_name = "t1={}_t2={}".format(t1_size, t2_size)
        output_dir_key = "{}/{}/{}/{}/{}".format(KEY_BASE, CUR_OUTPUT_SET, args.machineType, trace_name, exp_dir_name)

        remote_config_key = "{}/config.json".format(output_dir_key)
        
        if s3_client.check_prefix_exist(remote_config_key):
            print("Key {} already exist.".format(remote_config_key))
            continue 

        if not s3_client.check_prefix_exist(row["traceKey"]):
            print("Key {} does not exist.".format(row["traceKey"]))
            continue 

        new_args.t1Size = t1_size
        new_args.t2Size = t2_size 
        new_args.tracePath = "/tmp/{}".format(trace_name)

        print("running experiment ", new_args)

        if not Path(new_args.tracePath).exists():
            s3_client.download_s3_obj(row["traceKey"], new_args.tracePath)
        assert Path(new_args.tracePath).exists()

        replay_config = create_replay_config(new_args)
        config_file_path = args.statOutDir.joinpath("config.json")
        with config_file_path.open("w+") as config_handle:
            dump(replay_config, config_handle, indent=2)
        assert config_file_path.exists()

        print("config file uploaded to mark exp start")
        s3_client.upload_s3_obj(remote_config_key, str(config_file_path.absolute()))

        run_trace_replay(replay_config, new_args)

        print("experiment completed uploading files.")
        output_file_names = ["stat_0.out", "tsstat_0.out", "usage.csv", "power.csv"]
        for output_file_name in output_file_names:
            upload_key = "{}/{}".format(output_dir_key, output_file_name)
            output_file_path = args.statOutDir.joinpath(output_file_name)
            assert output_file_path.exists()
            s3_client.upload_s3_obj(upload_key, str(output_file_path.absolute()))

    return experiment_found


def main():
    parser = ArgumentParser(description="Run Block Trace Replay")

    parser.add_argument("--statOutDir",
                            "-s",
                            default=Path("/tmp/replay"),
                            type=Path,
                            help="Directory to store output from trace replay.")

    # parser.add_argument("--tracePath",
    #                         "-t",
    #                         type=str,
    #                         help="Path to cache trace to replay.")
    
    # parser.add_argument("--t1Size",
    #                         "-t1",
    #                         type=int,
    #                         help="Size of tier-1 cache.")

    # parser.add_argument("--t2Size",
    #                         "-t2",
    #                         type=int,
    #                         help="Size of tier-2 cache.")

    parser.add_argument("--backingFilePath",
                            "-b",
                            type=str,
                            help="Path to a file in backing store device.")
    
    parser.add_argument("--machineType",
                            "-m",
                            type=str,
                            help="The type of machine.")
    
    parser.add_argument("--nvmFilePath",
                            "-nvm",
                            type=str,
                            help="Path to a file in NVM device.")

    args = parser.parse_args()

    # make sure the output dir exists 
    args.statOutDir.mkdir(exist_ok=True, parents=True)


    while start_next_experiment(args):
        pass 



    # # if trace does not exist, create s3 key and download the trace 
    # assert Path(args.tracePath).exists() and Path(args.backingFilePath).exists() 

    # replay_config = create_replay_config(args)
    # run_trace_replay(replay_config, args)


if __name__ == "__main__":
    main()