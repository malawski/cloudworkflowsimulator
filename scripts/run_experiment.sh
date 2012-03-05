#!/bin/bash

if [ $# -lt 1 ]; then
	echo "Usage: $0 EXPERIMENT_PROPERTIES"
	exit 1
fi

INPUT_DIR=$1

INPUT=$( ls -1 $INPUT_DIR/input*.properties | head -n $SGE_TASK_ID | tail -n 1)

echo java -cp lib/cloudsim-2.1.1.jar:dist/cloudworkflowsimulator.jar cws.core.experiment.Experiment $INPUT
java -cp lib/cloudsim-2.1.1.jar:dist/cloudworkflowsimulator.jar cws.core.experiment.Experiment $INPUT