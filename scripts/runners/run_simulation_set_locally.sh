#!/bin/bash

if [ $# -lt 1 ]; then
        echo "Usage: $0 <input file>"
        exit 1
fi

INPUT=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cat $INPUT | xargs -n 1 -d "\n" ${DIR}/run_simulation_locally.sh

