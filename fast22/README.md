# MTCache - Block Cache Replay 

This folder contains scripts for running experiments using BlockCacheStressor. The input and output data are stored 
in an S3 bucket. 

## Setup 

The path to storage device used for backing storage and NVM caching is needed to setup the mounts. The credentials for AWS is needed to access input and output files in AWS S3. 

```
cd scripts 
./setup-all.sh /dev/sdb /dev/sdc AWS_ACCESS_KEY AWS_SECRET
```

## Run

A set of basic experiment can be for run a workload by using the run.sh script. 

```
    ./run.sh 
        cloudlab_a
        ~/disk/disk.path
        ~/nvm/disk.path
        w103
        0
```




