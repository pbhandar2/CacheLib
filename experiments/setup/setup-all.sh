#! /usr/bin/env bash

# {{{ Bash settings
# abort on nonzero exitstatus
set -o errexit
# abort on unbound variable
set -o nounset
# don't hide errors within pipes
set -o pipefail
# }}}

backing_dev_path=${1}
nvm_dev_path=${2}
backing_file_size_gb=${3}
nvm_file_size_gb=${4}

./setup-mount.sh ${backing_dev_path} ${nvm_dev_path} ${backing_file_size_gb} ${nvm_file_size_gb}

aws_access_key=${5}
aws_secret=${6}

./setup-aws.sh ${aws_access_key} ${aws_secret}

./setup-cachelib.sh 

