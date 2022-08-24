# Installation 

> **Note**
> In order to run MT experiments. You need a backing storage device and an NVM device. 

Clone the necessay repos 
```
git clone https://github.com/pbhandar2/MTCacheData.git
git clone https://github.com/pbhandar2/CacheLib.git
cd CacheLib
./contrib/build.sh -j -d 
```

Mount the backing storage and NVM device and create a large file on it. 
```
    echo "${BACKING_DIR} not mounted"
    mkfs -t ext4 ${backing_store_path}
    mount ${backing_store_path} ${BACKING_DIR}
    echo "${BACKING_DIR} mounted, now creating file ${BACKING_DIR}/disk.file"
    dd if=/dev/urandom of=${BACKING_DIR}/disk.file bs=1M count=500000 oflag=direct 
    chmod a+rwx ${BACKING_DIR}/disk.file
```

In directory, CacheLib/fast22/experiment/, edit the following fields in mt_config_template.json and st_config_template.json. 
- diskFilePath: path to a large file in the backing storage 
- nvmCachePaths: path to a large file in the NVM 


Download a block trace and its corresponding reuse distance histogram file. 

w93 block trace: https://www.dropbox.com/s/lmd94hhuayota24/w93.csv?dl=0

w93 RD histogram file: https://www.dropbox.com/s/uv6bhfgvpkrvz56/w93.csv?dl=0


Run the basic cache configurations using the 'run_basic_experiment.sh' script. 

| Argument  | Description |
| ------------- | ------------- |
| machine_id      | Machine identifier |
| disk_file_path  | Path to file on backing storage |
| nvm_file_path  | Path to file on NVM |
| workload_id  | Workload identifier |
| iteration  | Iteration count (0 to start)  |
| block_trace_path  | Path to block trace  |
| rd_hist_file_path | Path to hit reuse distance (RD) histogram file |
| output_dir | Path to the output directory (MTCacheData/data) |


EXAMPLE 

./run_basic_experiment.sh 
                 cloudlab_a 
                 /users/pbhandar/disk/disk.file 
                 /users/pbhandar/flash/disk.file 
                 /users/pbhandar/cp_traces/w66.csv 
                 /users/pbhandar/rd_hist_4k/w66.csv 
                 /users/pbhandar/MTCacheData/data 