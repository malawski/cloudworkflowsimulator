#!/bin/bash

if [ $# -lt 1 ]; then
        echo "Usage: $0 <input file>"
        exit 1
fi

INPUT=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if hash gxargs 2>/dev/null; then
  cat $INPUT | gxargs -n 1 -d "\n" ${DIR}/run_simulation_locally.sh
else
  cat $INPUT | xargs -n 1 -d "\n" ${DIR}/run_simulation_locally.sh
fi;

