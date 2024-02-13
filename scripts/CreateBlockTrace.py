from argparse import ArgumentParser 
from pathlib import Path 

from cydonia.profiler.CacheTraceProfiler import generate_block_trace


def main():
    parser = ArgumentParser(description="Create block trace from cache trace.")
    parser.add_argument("--cacheTracePath", "-c", type=str, help="Cache trace path.")
    parser.add_argument("--blockTracePath", "-b", type=str, help="Block trace path.")
    args = parser.parse_args()

    assert Path(args.cacheTracePath).exists()
    generate_block_trace(args.cache_trace_path, args.block_trace_path)


if __name__ == "__main__":
    main()