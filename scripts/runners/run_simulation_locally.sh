#!/bin/bash

if [ $# -lt 1 ]; then
        echo "Usage: $0 <simulation args>"
        exit 1
fi

ARGS=$1
echo $ARGS

java -cp "../../lib/*:../../bin/*:../../bin/" cws.core.algorithms.TestRun $ARGS
