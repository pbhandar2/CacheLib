#! /usr/bin/env bash
#
# Author: Pranav Bhandari bhandaripranav94@gmail.com
#
#/ Usage: run [OPTIONS]... [ARGUMENTS]...
#/ block_req_queue_depth              Maximum outstanding block requests at a time
#/ thread_count                       Number of threads each for processing block requests and completed async IO
#/ iat_scale_factor                   The factor by which the IAT(Inter Arrival Time) is divided
#/ iteration                          The iteration count of the experiment
#/ block_trace_path                   Path to block trace
#/ rd_hist_file_path                  Path to hit RD hist file
#/ output_dir                         Path to the output directory
#/ 
#/ OPTIONS
#/   -h, --help
#/                Print this help message
#/
#/ EXAMPLES
#/ ./run.sh 128 8 100 0 /home/trace/w103.csv /home/hit_rate/w103.csv /home/MTCache/mtdata
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
ARGUMENT_ARRAY=(
    "block_req_queue_depth" 
    "thread_count"
    "iat_scale_factor"
    "iteration"
    "block_trace_path"
    "rd_hist_file_path"
    "output_dir"
    "machine_tag"
    "workload_tag")
readonly ARGUMENT_ARRAY

DESCRIPTION_ARRAY=(
    "Maximum outstanding block requests at a time"
    "Number of threads each for processing block requests and completed async IO" 
    "The factor by which the IAT(Inter Arrival Time) is divided"
    "The iteration count of the experiment"
    "Path to block trace"
    "Path to hit rate file"
    "Path to the output directory"
    "Tag to identify the machine"
    "Tag to identify the workload")
readonly DESCRIPTION_ARRAY

POSSIBLE_QUEUE_SIZE_ARRAY=(64 128 256)
POSSIBLE_THREAD_COUNT_ARRAY=(8 16)
POSSIBLE_IAT_SCALE_FACTOR_ARRAY=(1 100)
POSSIBLE_T1_HIT_RATE=(40 60 80)
POSSIBLE_T2_HIT_RATE=(20 40 60)

argument_count=${#ARGUMENT_ARRAY[@]}
description_count=${#DESCRIPTION_ARRAY[@]}

script_name=$(basename "${0}")
script_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
readonly script_name script_dir

TEMP_CSV_FILE_NAME="temp_t1_t2.csv"
readonly TEMP_CSV_FILE_NAME

TEMP_CONFIG="temp_config.json"
readonly TEMP_CONFIG
#}}}

main() {
    check_and_load_args "${@}"
    run_experiment_set ${1} ${2} ${3} ${4} ${5} ${6} ${7} ${8} ${9} 
}

# {{{ Helper functions

run_base_experiment() {
    ## create a config 
    python3 make_config.py ${1} ${2} ${3} ${4} ${5} ${6} ${7} 

    # output file path
    echo "RUN: ${1} ${2} ${3} ${4} ${5} ${6} ${7} ${8}"

    cat ${7}

    ../../opt/cachelib/bin/cachebench --json_test_config ${7} > ${8}

    echo "Experiment Done!"
}


run_experiment_set() {
    queue_size=${1}
    thread_count=${2}
    iat_scale_factor=${3}
    iteration=${4}
    block_trace_path=${5}
    rd_hist_file_path=${6}
    output_dir=${7}
    machine_tag=${8}
    workload_tag=${9}

    # create necessary directories
    full_output_dir="${output_dir}/${machine_tag}/${workload_tag}"
    mkdir -p ${full_output_dir}

    echo "Output directory: ${full_output_dir}"

    t1_size_str=""
    for t1_hr in ${POSSIBLE_T1_HIT_RATE[@]}
    do 
        t1_size_str="${t1_size_str} ${t1_hr}"
    done 

    # generate t1,t2 size combinations from the hit rate 
    python3 generate_t1_t2.py ${rd_hist_file_path} ${TEMP_CSV_FILE_NAME} --t1_hit_rate ${POSSIBLE_T1_HIT_RATE[@]} --t2_hit_rate ${POSSIBLE_T2_HIT_RATE[@]}

    # run experiment for each t1, t2 size combination 
    t1_size=0
    t2_size=0
    experiment_count=0
    while IFS=, read -r t1_size t2_size ; do 
        output_file_name="${queue_size}_${thread_count}_${iat_scale_factor}_${t1_size}_${t2_size}_${iteration}.dump"
        output_path="${full_output_dir}/${output_file_name}"
        # check if path exists 
        if [ ! -f "${output_path}" ]; then
            echo "Stat dump (${output_path}) does not exist."
            run_base_experiment ${queue_size} ${thread_count} ${iat_scale_factor} ${block_trace_path} ${t1_size} ${t2_size} ${TEMP_CONFIG} ${output_path}
        else 
            echo "Stat dump (${output_path}) exist."
        fi
        experiment_count=$((${experiment_count}+1))
        sleep 15 # 15 second break after every experiment 
    done < ${TEMP_CSV_FILE_NAME}

    if [[ ${experiment_count} > 0 ]]; then 
        output_file_name="${queue_size}_${thread_count}_${iat_scale_factor}_${t1_size}_${t2_size}_${iteration}.dump"
        output_path="${full_output_dir}/${output_file_name}"
        # check if path exists 
        if [ ! -f "${output_path}" ]; then
            echo "Stat dump (${output_path}) does not exist."
            run_base_experiment ${queue_size} ${thread_count} ${iat_scale_factor} ${block_trace_path} ${t1_size} ${t2_size} ${TEMP_CONFIG} ${output_path}
        else 
            echo "Stat dump (${output_path}) exist."
        fi

        rm ${TEMP_CSV_FILE_NAME}
        rm ${TEMP_CONFIG}
    fi
}

print_args_and_descriptions() {
    printf "Arguments\n---------\n"
    for ((i=0 ; i<${argument_count}; i++)); do
        printf "%-35s%s\n" "${ARGUMENT_ARRAY[${i}]}" "${DESCRIPTION_ARRAY[${i}]}"
    done
}

display_help() {
    echo "Usage: $0 queue_size thread_count iat_scale_factor iteration" >&2
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

    if [[ ! " ${POSSIBLE_QUEUE_SIZE_ARRAY[*]} " =~ " ${1} " ]]; then
        printf "queue size cannot be ${1}, values allowed:" 
        for queue_size in ${POSSIBLE_QUEUE_SIZE_ARRAY[@]}
        do 
            printf "%s " $queue_size
        done
        echo 
        exit
    fi

    if [[ ! " ${POSSIBLE_THREAD_COUNT_ARRAY[*]} " =~ " ${2} " ]]; then
        printf "thread count cannot be ${2}, values allowed:" 
        for cur_thread_count in ${POSSIBLE_THREAD_COUNT_ARRAY[@]}
        do 
            printf "%s " $cur_thread_count
        done
        echo 
        exit
    fi

    if [[ ! " ${POSSIBLE_IAT_SCALE_FACTOR_ARRAY[*]} " =~ " ${3} " ]]; then
        printf "IAT scale factor count cannot be ${3}, values allowed:" 
        for cur_iat_scale_factor in ${POSSIBLE_IAT_SCALE_FACTOR_ARRAY[@]}
        do 
            printf "%s " $cur_iat_scale_factor
        done
        echo 
        exit
    fi

    if [ ! -f "${5}" ]; then
        echo "Block trace file (${5}) does not exist."
        exit
    fi

    if [ ! -f "${6}" ]; then
        echo "Hit rate file (${6}) does not exist."
        exit
    fi

    if [ ! -d "${7}" ]; then
        echo "Output directory (${7}) does not exist."
        exit
    fi
}

# }}} Helper functions

main "${@}"

