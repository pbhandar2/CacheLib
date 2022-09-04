#! /usr/bin/env bash

# {{{ Bash settings
# abort on nonzero exitstatus
set -o errexit
# abort on unbound variable
set -o nounset
# don't hide errors within pipes
set -o pipefail
# }}}


# {{{ Variables

BACKING_DIR="${HOME}/disk"
NVM_DIR="${HOME}/nvm"
BACKING_FILE_SIZE_MB=900000 # 900 GB
NVM_CACHE_SIZE_MB=410000 # 

if [[ ! -d ${BACKING_DIR} ]]; then 
    mkdir ${BACKING_DIR}
fi 

if [[ ! -d ${NVM_DIR} ]]; then 
    mkdir ${NVM_DIR}
fi 

# }}}


backing_dev_path=${1}
nvm_dev_path=${2}

if mountpoint -q ${BACKING_DIR}; then
    echo "${BACKING_DIR} already mounted"
    # check if the disk.file exists and the size of the file 
else
    echo "${BACKING_DIR} not mounted"
    mkfs -t ext4 ${backing_dev_path}
    mount ${backing_dev_path} ${BACKING_DIR}
    echo "${BACKING_DIR} mounted, now creating file ${BACKING_DIR}/disk.file"
    # 500 GB file 
    dd if=/dev/urandom of=${BACKING_DIR}/disk.file bs=1M count=${BACKING_FILE_SIZE_MB} oflag=direct 
    chmod a+rwx ${BACKING_DIR}/disk.file
fi

if mountpoint -q ${NVM_DIR}; then
    echo "${NVM_DIR} already mounted"
    # check if the disk.file exists and the size of the file 
else
    echo "${NVM_DIR} not mounted"
    mkfs -t ext4 ${nvm_dev_path}
    mount ${nvm_dev_path} ${NVM_DIR}
    echo "${NVM_DIR} mounted, now creating file ${NVM_DIR}/disk.file"
    # 200 GB cache file 
    dd if=/dev/urandom of=${NVM_DIR}/disk.file bs=1M count=${NVM_CACHE_SIZE_MB} oflag=direct 
    chmod a+rwx ${NVM_DIR}/disk.file
fi