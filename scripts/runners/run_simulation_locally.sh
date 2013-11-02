#!/bin/bash

if [ $# -lt 1 ]; then
        echo "Usage: $0 <simulation args>"
        exit 1
fi

ARGS=$1
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo java -cp "${DIR}/../../lib/*:${DIR}/../../bin/*:${DIR}/../../bin/" cws.core.algorithms.TestRun $ARGS
java -cp "${DIR}/../../lib/*:${DIR}/../../bin/*:${DIR}/../../bin/" cws.core.algorithms.TestRun $ARGS
