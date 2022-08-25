# MTCache - Block Cache Replay 

This folder contains scripts for running experiments using BlockCacheStressor. The input and output data are stored 
in an S3 bucket. 

## Setup 

There are two scirpts for setup: setup-cachelib and setup-mount. setup-mount mounts the two storage devices: NVM and backing store. Once the devices are mounted, a large file is created where the IO from the experiments are directed. 

```
cd scripts 
./setup-mount.sh /dev/sdb /dev/sdc 
./setup-cachelib.sh 
```

## Run

Credentials for an AWS account is required to run the experiments. Input and output data is read from S3 buckets. 

```
    ./run.sh 
        ${AWS_ACCOUNT_NAME}
        ${AWS_ACCESS_KEY}
        ${AWS_SECRET}
```




