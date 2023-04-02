#! /usr/bin/env bash

# {{{ Bash settings
# abort on nonzero exitstatus
set -o errexit
# abort on unbound variable
set -o nounset
# don't hide errors within pipes
set -o pipefail
# }}}

AWS_URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"
AWS_INSTALL_ZIP="${HOME}/awscliv2.zip"
INSTALL_DIR="${HOME}"

if [[ ! -f ${AWS_INSTALL_ZIP} ]]; then 
    rm -rf ${AWS_INSTALL_ZIP}
fi 
if [[ ! -d ${INSTALL_DIR} ]]; then 
    rm -rf ${INSTALL_DIR}
fi 

curl ${AWS_URL} -o ${AWS_INSTALL_ZIP}
unzip ${AWS_INSTALL_ZIP} -d ${INSTALL_DIR}
sudo "${INSTALL_DIR}/aws/install" --update
rm -rf ${AWS_INSTALL_ZIP}
rm -rf "${INSTALL_DIR}/aws"

aws_access_key=${1}
aws_secret=${2}

aws configure set aws_access_key_id ${aws_access_key}
aws configure set aws_secret_access_key ${aws_secret}