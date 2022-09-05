#! /usr/bin/env bash
#
# Author: Pranav Bhandari bhandaripranav94@gmail.com
#
#/ Usage: unit_experiment.sh [OPTIONS]... [ARGUMENTS]...


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
    "experiment_id"
    "machine_id"
    "workload_id"
    "aws_access_key"
    "aws_secret_key")

readonly DESCRIPTION_ARRAY=(
    "Experiment identification"
    "Machine identification"
    "Workload identification"
    "AWS access key"
    "AWS secrety key")

readonly TEMP_CSV_FILE_NAME="${HOME}/temp_t1_t2.csv"
readonly TEMP_CONFIG="${HOME}/temp_config.json"
readonly TEMP_OUT="${HOME}/temp_out.dump"

readonly argument_count=${#ARGUMENT_ARRAY[@]}
readonly description_count=${#DESCRIPTION_ARRAY[@]}

readonly script_name=$(basename "${0}")
readonly script_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# }}}


download_from_s3() {
    block_trace_key=${1}
    output_path=${2}
    python3 ./s3_download.py ${block_trace_key} ${output_path}
}


main() {
    experiment_list_file=$1

    while IFS=, read -r experiment_id machine_id workload_id queue_size thread_count iat_scale_factor t1_size_mb t2_size_mb iteration_count upload_key; do
        block_trace_path="${HOME}/${workload_id}.csv"
        block_trace_key="block_trace/${workload_id}.csv"
        if [ ! -f "${block_trace_path}" ]; then
            download_from_s3 ${block_trace_key} ${block_trace_path}
            echo "Block trace downloaded at ${block_trace_path}"
        else 
            echo "Block trace exists at ${block_trace_path}"
        fi 

        # TODO: cleanup if block script still doesn't exist due to some reason 
        if [ ! -f "${block_trace_path}" ]; then
            echo "Block trace could not be downloaded"
            exit
        fi

        

        ## create a config 
        python3 make_config.py ${queue_size} \
                                ${thread_count} \
                                ${iat_scale_factor} \
                                ${block_trace_path} \
                                ${t1_size_mb} \
                                ${t2_size_mb} \
                                ${TEMP_CONFIG} 
        
        echo "
        RUNNING SETUP
        ------------------------------------------------------
        Experiment name: ${experiment_id}
        Machine name: ${machine_id}
        Workload name: ${workload_id}
        Queue size: ${queue_size}
        Thread count: ${thread_count}
        IAT scale factor: ${iat_scale_factor}
        Iteration count: ${iteration_count}
        T1 size MB: ${t1_size_mb}
        T2 size MB: ${t2_size_mb}
        Output key: ${upload_key}
        "

        ../../opt/cachelib/bin/cachebench --json_test_config ${TEMP_CONFIG} > ${TEMP_OUT}

        python3 ./s3_upload.py ${upload_key} ${TEMP_OUT}

        echo "Experiment Done! Key: ${upload_key}"


    done < ${experiment_list_file}

}

main "${@}"