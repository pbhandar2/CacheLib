from argparse import ArgumentParser 
from pathlib import Path 
from json import dump 

from ReplayConfig import ReplayConfig
from Runner import Runner 


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

    with config_file_path.open("w+") as config_handle:
        dump(replay_config, config_handle, indent=2)
    
    assert config_file_path.exists()
    
    stdout_path = str(args.statOutDir.joinpath("stdout.dump").absolute())
    stderr_path = str(args.statOutDir.joinpath("stderr.dump").absolute())
    usage_path = args.statOutDir.joinpath("usage.csv")
    power_path = args.statOutDir.joinpath("power.csv")

    runner = Runner()
    runner.run(run_cmd, stdout_path, stderr_path, usage_path, power_path)


def main():
    parser = ArgumentParser(description="Run Block Trace Replay")

    parser.add_argument("--statOutDir",
                            "-s",
                            default=Path("/tmp/replay"),
                            type=Path,
                            help="Directory to store output from trace replay.")

    parser.add_argument("--tracePath",
                            "-t",
                            type=str,
                            help="Path to cache trace to replay.")
    
    parser.add_argument("--t1Size",
                            "-t1",
                            type=int,
                            help="Size of tier-1 cache.")

    parser.add_argument("--t2Size",
                            "-t2",
                            type=int,
                            help="Size of tier-2 cache.")

    parser.add_argument("--backingFilePath",
                            "-b",
                            type=str,
                            help="Path to a file in backing store device.")
    
    parser.add_argument("--nvmFilePath",
                            "-nvm",
                            type=str,
                            required=False, 
                            help="Path to a file in NVM device.")

    args = parser.parse_args()

    # make sure the output dir exists 
    args.statOutDir.mkdir(exist_ok=True, parents=True)

    assert Path(args.tracePath).exists() and Path(args.backingFilePath).exists() 

    replay_config = create_replay_config(args)
    run_trace_replay(replay_config, args)


if __name__ == "__main__":
    main()