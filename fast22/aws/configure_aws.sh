aws_access_key=${1}
aws_secret=${2}

aws configure set aws_access_key_id ${aws_access_key}
aws configure set aws_secret_access_key ${aws_secret}