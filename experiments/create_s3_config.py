import sys 
import json 

s3_key = sys.argv[1]
s3_secret = sys.argv[2]

s3_config = {
    "key": s3_key,
    "secret": s3_secret
}

with open("s3_config.json", "w+") as f:
    json.dump(s3_config, f)