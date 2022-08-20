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
#/ hit_rate_file_path                 Path to hit rate file
#/ 
#/ OPTIONS
#/   -h, --help
#/                Print this help message
#/
#/ EXAMPLES
#/ ./run.sh 
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
    "hit_rate_file_path")
readonly ARGUMENT_ARRAY

DESCRIPTION_ARRAY=(
    "Maximum outstanding block requests at a time"
    "Number of threads each for processing block requests and completed async IO" 
    "The factor by which the IAT(Inter Arrival Time) is divided"
    "The iteration count of the experiment"
    "Path to block trace"
    "Path to hit rate file")
readonly DESCRIPTION_ARRAY

POSSIBLE_QUEUE_SIZE_ARRAY=(64 128 256)
POSSIBLE_THREAD_COUNT_ARRAY=(8 16)
POSSIBLE_IAT_SCALE_FACTOR_ARRAY=(1 100)
POSSIBLE_T1_HIT_RATE=(40,60,80)
POSSIBLE_T2_HIT_RATE=(20,40,60)

argument_count=${#ARGUMENT_ARRAY[@]}
description_count=${#DESCRIPTION_ARRAY[@]}

script_name=$(basename "${0}")
script_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
readonly script_name script_dir
#}}}

main() {
    check_and_load_args "${@}"
    run_experiment_set ${1} ${2} ${3} ${4} ${5} ${6}
}

# {{{ Helper functions

run_base_experiment() {
    echo "Running queue_size=${queue_size}, thread_count=${thread_count}, iat_scale_factor=${iat_scale_factor}, iteration=${iteration}, block_trace_path=${block_trace_path}, t1_size=${1}, t2_size=${2}"

    ## create a config 

    ## run the experiment 

    # output file path
}


run_experiment_set() {
    queue_size=${1}
    thread_count=${2}
    iat_scale_factor=${3}
    iteration=${4}
    block_trace_path=${5}
    hit_rate_file_path=${6}

    # generate the expected output path 

    # check if path exists 

    # call a python function to generate cache size
    python3 generate_t1_t2.py 

    # generate t1, t2 sizes to be run and store it in file using a python script 
    while IFS=, read -r t1_size t2_size ; do 
        run_base_experiment ${t1_size} ${t2_size}
    done < sample_t1_t2.csv
    run_base_experiment ${t1_size} ${t2_size}

    # for each row in the file containing t1, t2 sizes, we run a experiment 
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
        echo "Hit rate file (${5}) does not exist."
        exit
    fi
}

# }}} Helper functions

main "${@}"

