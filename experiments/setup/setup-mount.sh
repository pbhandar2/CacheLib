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

if [[ ! -d ${BACKING_DIR} ]]; then 
    mkdir ${BACKING_DIR}
fi 

if [[ ! -d ${NVM_DIR} ]]; then 
    mkdir ${NVM_DIR}
fi 

# }}}


backing_dev_path=${1}
nvm_dev_path=${2}
backing_file_size_gb=${3}
nvm_file_size_gb=${4} 
backing_file_size_byte=$(( backing_file_size_gb * 1024 * 1024 * 1024))
nvm_file_size_byte=$(( nvm_file_size_gb * 1024 * 1024 * 1024))

backing_file_path="${BACKING_DIR}/disk.file"
if mountpoint -q ${BACKING_DIR}; then
    echo "${BACKING_DIR} already mounted" 
else
    echo "${BACKING_DIR} not mounted"
    mkfs -t ext4 ${backing_dev_path}
    mount ${backing_dev_path} ${BACKING_DIR}
    echo "${BACKING_DIR} mounted, now creating file ${backing_file_path}"
fi

create_file_flag=0
if [[ -f ${backing_file_path} ]]; then
    echo "${backing_file_path} already exists!"
    cur_filesize=$(wc -c ${backing_file_path} | awk '{print $1}')
    echo "Size is ${cur_filesize} and required is ${backing_file_size_byte}"
    if (( backing_file_size_byte > cur_filesize )); then
        create_file_flag=1
    fi 
else
    create_file_flag=1
fi 

if (( create_file_flag > 0 )); then
    echo "Creating backing file of size ${backing_file_size_gb} MB"
    dd if=/dev/urandom of=${backing_file_path} bs=1M count=$((${backing_file_size_gb}*1024)) oflag=direct 
    chmod a+rwx ${BACKING_DIR}/disk.file
fi 


nvm_file_path="${NVM_DIR}/disk.file"
if mountpoint -q ${NVM_DIR}; then
    echo "${NVM_DIR} already mounted"
else
    echo "${NVM_DIR} not mounted"
    mkfs -t ext4 ${nvm_dev_path}
    mount ${nvm_dev_path} ${NVM_DIR}
    echo "${NVM_DIR} mounted, now creating file ${nvm_file_path}"
fi

# check if NVM file path already exists and that the file is large enough 
create_file_flag=0
if [[ -f ${nvm_file_path} ]]; then
    cur_filesize=$(wc -c ${nvm_file_path} | awk '{print $1}')
    echo "${nvm_file_path} already exists."
    echo "Size is ${cur_filesize} and required is ${nvm_file_size_byte}"
    if (( nvm_file_size_byte > cur_filesize )); then
        create_file_flag=1
    fi 
else
    create_file_flag=1
fi 

if (( create_file_flag > 0 )); then
    echo "Creating file ${nvm_file_path} of size ${nvm_file_size_gb} GB"
    dd if=/dev/urandom of=${nvm_file_path} bs=1M count=$((${nvm_file_size_gb}*1024)) oflag=direct 
    chmod a+rwx ${nvm_file_path}
fi 