#! /usr/bin/env bash
#
# Author: Pranav Bhandari bhandaripranav94@gmail.com
#
#/ Usage: run.sh [OPTIONS]... [ARGUMENTS]...

#/ ARGUMENTS                            DESCRIPTION
#/ -------------------------------------------------------------------------------------
#/ machine_id                           Machine identifier
#/ disk_file_path                       Path to file on disk  
#/ nvm_file_path                        Path to file on NVM
#/ workload_id                          Workload identifier
#/ iteration                            Iteration count 
#/ 
#/ OPTIONS
#/   -h, --help
#/                Print this help message
#/
#/ EXAMPLE
#/ ./run.sh 
#/                  cloudlab_a 
#/                  /users/pbhandar/disk/disk.file 
#/                  /users/pbhandar/flash/disk.file 
#/                  w66.csv
#/                  0
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
    "machine_id" 
    "disk_file_path"
    "nvm_file_path"
    "workload_id"
    "iteration")

readonly DESCRIPTION_ARRAY=(
    "Machine identifier"
    "Path to file on disk" 
    "Path to file on NVM"
    "workload_id"
    "Iteration count")

readonly TEMP_CSV_FILE_NAME="${HOME}/temp_t1_t2.csv"
readonly TEMP_CONFIG="${HOME}/temp_config.json"
readonly TEMP_OUT="${HOME}/temp_out.dump"

readonly POSSIBLE_QUEUE_SIZE_ARRAY=(128 256)
readonly POSSIBLE_THREAD_COUNT_ARRAY=(8 16)
readonly POSSIBLE_IAT_SCALE_FACTOR_ARRAY=(1 100)
readonly POSSIBLE_T1_HIT_RATE=(40 60 80)
readonly POSSIBLE_T2_HIT_RATE=(20 40 60)

readonly argument_count=${#ARGUMENT_ARRAY[@]}
readonly description_count=${#DESCRIPTION_ARRAY[@]}

readonly script_name=$(basename "${0}")
readonly script_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# }}}


main() {
    check_and_load_args "${@}"
    echo "Arg check passed!"

    machine_id=${1}
    disk_file_path=${2}
    nvm_file_path=${3}
    workload_id=${4}
    iteration=${5}

    block_trace_path="${HOME}/block_${workload_id}.csv"
    block_trace_key="block_trace/${workload_id}.csv"
    if [ ! -f "${block_trace_path}" ]; then
        download_from_s3 ${block_trace_key} ${block_trace_path}
        echo "Block trace downloaded at ${block_trace_path}"
    else 
        echo "Block trace exists at ${block_trace_path}"
    fi 

    rd_hist_file_path="${HOME}/rd_hist_${workload_id}.csv"
    rd_hist_key="rd_hist_4k/${workload_id}.csv"
    if [ ! -f "${rd_hist_file_path}" ]; then
        download_from_s3 ${rd_hist_key} ${rd_hist_file_path}
        echo "RD hist file downloaded at ${rd_hist_file_path}"
    else 
        echo "RD hist file exists at ${rd_hist_file_path}"
    fi 

    run_basic_experiment ${machine_id} \
                            ${disk_file_path} \
                            ${nvm_file_path} \
                            ${workload_id} \
                            ${iteration} \
                            ${block_trace_path} \
                            ${rd_hist_file_path} 
}


# {{{ Helper functions


download_from_s3() {
    block_trace_key=${1}
    output_path=${2}
    python3 ./aws/s3_download.py ${block_trace_key} ${output_path}
}


run_unit_experiment() {
    queue_size=${1}
    thread_count=${2}
    iat_scale_factor=${3}
    block_trace_file=${4}
    t1_size_mb=${5}
    t2_size_mb=${6}
    config_path=${7}
    output_key=${8}

    ## create a config 
    python3 make_config.py ${queue_size} \
                            ${thread_count} \
                            ${iat_scale_factor} \
                            ${block_trace_file} \
                            ${t1_size_mb} \
                            ${t2_size_mb} \
                            ${config_path} 

    # output file path
    echo "RUNNING SETUP
    ------------------------------------------------------
    Queue size: ${queue_size},
    Thread count: ${thread_count},
    IAT scale factor: ${iat_scale_factor},
    Block trace: ${block_trace_file},
    T1 size MB: ${t1_size_mb},
    T2 size MB: ${t2_size_mb},
    Config path: ${config_path},
    Output key: ${output_key}"

    ../opt/cachelib/bin/cachebench --json_test_config ${config_path} > ${TEMP_OUT}

    python3 ./aws/s3_upload.py ${output_key} ${TEMP_OUT}

    echo "Experiment Done!"
}


run_basic_experiment() {
    machine_id=${1}
    disk_file_path=${2}
    nvm_file_path=${3}
    workload_id=${4}
    iteration=${5}
    block_trace_path=${6}
    rd_hist_file_path=${7}
 
    output_key_base="output/${machine_id}/${workload_id}"
    for queue_size in ${POSSIBLE_QUEUE_SIZE_ARRAY[@]}
    do
        for thread_count in ${POSSIBLE_THREAD_COUNT_ARRAY[@]}
        do 
            for iat_scale_factor in ${POSSIBLE_IAT_SCALE_FACTOR_ARRAY[@]}
            do 
                t1_size=0
                t2_size=0
                experiment_count=0
                output_sub_key="${output_key_base}/${queue_size}_${thread_count}_${iat_scale_factor}"
                # generate t1,t2 size combinations from the hit rate 
                python3 generate_t1_t2_sizes.py \
                            ${rd_hist_file_path} \
                            ${TEMP_CSV_FILE_NAME} \
                            --t1_hit_rate ${POSSIBLE_T1_HIT_RATE[@]} \
                            --t2_hit_rate ${POSSIBLE_T2_HIT_RATE[@]} \
                            --output_key_partial ${output_sub_key} \
                            --iteration ${iteration}

                if [[ -f ${TEMP_CSV_FILE_NAME} ]]; then 
                    while IFS=, read -r t1_size t2_size ; do
                        output_file_name="${queue_size}_${thread_count}_${iat_scale_factor}_${t1_size}_${t2_size}_${iteration}"
                        output_key="${output_key_base}/${output_file_name}"

                        echo "Running ${output_key}"

                        run_unit_experiment ${queue_size} \
                                                ${thread_count} \
                                                ${iat_scale_factor} \
                                                ${block_trace_path} \
                                                ${t1_size} \
                                                ${t2_size} \
                                                ${TEMP_CONFIG} \
                                                ${output_key}

                        experiment_count=$((${experiment_count}+1))
                        sleep 15 # 15 second break after every experiment 
                    done < ${TEMP_CSV_FILE_NAME}
                fi
            done 
        done 
    done

    if [[ ${experiment_count} > 0 ]]; then 
        output_file_name="${queue_size}_${thread_count}_${iat_scale_factor}_${t1_size}_${t2_size}_${iteration}"
        output_key="${output_key_base}/${output_file_name}"

        echo "Running ${output_key}"

        run_base_experiment ${queue_size} \
                                ${thread_count} \
                                ${iat_scale_factor} \
                                ${block_trace_path} \
                                ${t1_size} \
                                ${t2_size} \
                                ${TEMP_CONFIG} \
                                ${output_key}

        rm ${TEMP_CSV_FILE_NAME}
        rm ${TEMP_CONFIG}
        rm ${TEMP_OUT}
    fi
}

print_args_and_descriptions() {
    printf "Arguments\n---------\n"
    for ((i=0 ; i<${argument_count}; i++)); do
        printf "%-35s%s\n" "${ARGUMENT_ARRAY[${i}]}" "${DESCRIPTION_ARRAY[${i}]}"
    done
}

display_help() {
    echo "Usage: $0 machine_id disk_file_path nvm_file_path iteration block_trace_path rd_hist_file_path output_dir" >&2
    print_args_and_descriptions
    echo
    exit 1
}

check_argument_and_description_array_size() {
    if [[ $argument_count != $description_count ]];
    then 
        echo "argument_count: ${argument_count} should be equal to description_count: ${description_count}"
        exit
    fi
}

check_and_load_args() {
    # print help if necessary 
    while getopts ":h" option; do
    case $option in
        h) # display Help
            display_help
            exit;;
    esac
    done

    check_argument_and_description_array_size
    if [[ ${#} != ${argument_count} ]]
    then
        echo "Not enough arguments, should be ${argument_count} but is ${#}"
        display_help 
        exit
    fi

    if [ ! -f "${2}" ]; then
        echo "Disk file path (${2}) does not exist."
        exit
    fi

    if [ ! -f "${3}" ]; then
        echo "NVM file path (${3}) does not exist."
        exit
    fi

}
# }}} Helper functions

main "${@}"