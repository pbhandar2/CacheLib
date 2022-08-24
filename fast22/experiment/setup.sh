#! /usr/bin/env bash
#
# Author: Pranav Bhandari bhandaripranav94@gmail.com
#
#/ Usage: setup.sh [OPTIONS]... [ARGUMENTS]...
#/ ARGUMENTS                            DESCRIPTION
#/ -------------------------------------------------------------------------------------
#/ backing_store_path                    Path to the backing storage  
#/ nvm_store_path                        Path to NVM storage         
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
    "backing_store_path",
    "nvm_store_path")

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
    backing_store_path=${1}
    nvm_store_path=${2}

    setup_mounts ${backing_store_path} ${nvm_store_path}
}


# {{{ Helper functions

setup_mounts() {
    backing_store_path=${1}
    nvm_store_path=${2}

    if mountpoint -q ${BACKING_DIR}; then
        echo "${BACKING_DIR} already mounted"
    else
        mkfs -t ext4 ${backing_store_path}
        mount ${backing_store_path} ${BACKING_DIR}
    fi

    if mountpoint -q ${NVM_DIR}; then
        echo "${NVM_DIR} already mounted"
    else
        mkfs -t ext4 ${backing_store_path}
        mount ${backing_store_path} ${NVM_DIR}
    fi
}
# }}}

main "${@}"



# download CacheLib 
# install libaio 
# download MTCacheData 
# create the disk file 
# create the flash file 
# set the proper permission for disk and flash file 
# download the necessary trace 
# download the necessary rd hist file 
# set the proper permission for trace and rd hist file 

# build CacheLib 
# run the basic experiment script 
