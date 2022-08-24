Installation 

Clone the block stressor repo 

git clone https://github.com/pbhandar2/CacheLib.git

Clone the data repo 

git clone https://github.com/pbhandar2/MTCacheData.git

mount the backing storage device 
mount the NVM device 

go to Cachelib/fast22/experiments/

edit mt_config_template.json and st_config_template.json 
- diskFilePath: path to a large file in the mount used as a backing store 
- nvmCachePaths: path to a large file in the NVM mount 
- traceList: path to the block trace 

now you can run the basic experiment using the scirpt, run_basic_experiment.sh 

| Argument  | Description |
| ------------- | ------------- |
| machine_id      | Machine identifier |
| disk_file_path  | Path to file on disk |
| nvm_file_path  | Path to file on NVM |
| workload_id  | Workload identifier |
| iteration  | Iteration count (0 to start)  |
| block_trace_path  | Path to block trace  |
| rd_hist_file_path | Path to hit reuse distance (RD) histogram file |
| output_dir | Path to the output directory |


EXAMPLE \\
./run_basic_experiment.sh \\
                 cloudlab_a \\
                 /users/pbhandar/disk/disk.file \\
                 /users/pbhandar/flash/disk.file \\
                 /users/pbhandar/cp_traces/w66.csv \\
                 /users/pbhandar/rd_hist_4k/w66.csv \\
                 /users/pbhandar/MTCacheData/data \\