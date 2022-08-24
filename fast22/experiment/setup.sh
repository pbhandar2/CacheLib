#! /usr/bin/env bash
#
# Author: Pranav Bhandari bhandaripranav94@gmail.com
#
#/ Usage: setup.sh [OPTIONS]... [ARGUMENTS]...
#/ ARGUMENTS                            DESCRIPTION
#/ -------------------------------------------------------------------------------------
#/ backing_dev_path                    Path to the backing storage  
#/ nvm_dev_path                        Path to NVM storage   
#/ block_trace_url                       URL to the block trace file 
#/ rd_hist_file_url                      URL to the file containing the RD histogram   
#/ workload_tag                          Tag to identify the workload     
#/ 
#/ OPTIONS
#/   -h, --help
#/                Print this help message
#/
#/ EXAMPLE
#/ ./setup.sh 
#/                  /dev/sda
#/                  /dev/sdb
#/ 


# {{{ Bash settings
# abort on nonzero exitstatus
set -o errexit
# abort on unbound variable
set -o nounset
# don't hide errors within pipes
set -o pipefail
# }}}


# {{{ Variables

readonly ARGUMENT_ARRAY=(
    "backing_dev_path",
    "nvm_dev_path")

readonly DESCRIPTION_ARRAY=(
    "Path to the backing storage"
    "Path to NVM storage")

BACKING_DIR="${HOME}/disk"
NVM_DIR="${HOME}/nvm"

if [[ ! -d ${BACKING_DIR} ]]; then 
    mkdir ${BACKING_DIR}
fi 

if [[ ! -d ${NVM_DIR} ]]; then 
    mkdir ${NVM_DIR}
fi 

readonly script_name=$(basename "${0}")
readonly script_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# }}}


main() {
    backing_dev_path=${1}
    nvm_dev_path=${2}
    block_trace_url=${3}
    rd_hist_url=${4}
    workload_id=${5}

    setup_mounts ${backing_dev_path} ${nvm_dev_path}
    setup_cachelib 
    setup_mt_cache_data
}


# {{{ Helper functions

setup_mounts() {
    backing_dev_path=${1}
    nvm_dev_path=${2}

    if mountpoint -q ${BACKING_DIR}; then
        echo "${BACKING_DIR} already mounted"
    else
        echo "${BACKING_DIR} not mounted"
        mkfs -t ext4 ${backing_dev_path}
        mount ${backing_dev_path} ${BACKING_DIR}
        echo "${BACKING_DIR} mounted, now creating file ${BACKING_DIR}/disk.file"
        dd if=/dev/urandom of=${BACKING_DIR}/disk.file bs=1M count=500000 oflag=direct 
        chmod a+rwx ${BACKING_DIR}/disk.file
    fi

    if mountpoint -q ${NVM_DIR}; then
        echo "${NVM_DIR} already mounted"
    else
        echo "${NVM_DIR} not mounted"
        mkfs -t ext4 ${nvm_dev_path}
        mount ${nvm_dev_path} ${NVM_DIR}
        echo "${NVM_DIR} mounted, now creating file ${NVM_DIR}/disk.file"
        dd if=/dev/urandom of=${NVM_DIR}/disk.file bs=1M count=200000 oflag=direct 
        chmod a+rwx ${NVM_DIR}/disk.file
    fi
}

setup_cachelib() {
    sudo apt-get update 
    sudo apt install libaio-dev 
    cd ~
    if [ ! -d "${HOME}/CacheLib" ]; then
        git clone https://github.com/pbhandar2/CacheLib
    fi
    cd CacheLib 
    git checkout replay 
    ./contrib/build.sh -j -d 
}

setup_mt_cache_data() {
    cd ~
    if [ ! -d "${HOME}/MTCacheData" ]; then
        git clone https://github.com/pbhandar2/MTCacheData
    fi
}

# }}}

main "${@}"


# download the necessary trace 
# download the necessary rd hist file 
# set the proper permission for trace and rd hist file 

# build CacheLib 
# run the basic experiment script 
