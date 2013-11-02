#!/bin/bash

if [ $# -lt 1 ]; then
	echo "Usage: $0 EXPERIMENT_PROPERTIES"
	exit 1
fi

INPUT_LIST=$1

INPUT=$( cat $INPUT_LIST | head -n $SGE_TASK_ID | tail -n 1)

echo java -cp lib/cloudsim-2.1.1.jar:dist/cloudworkflowsimulator.jar cws.core.experiment.Experiment $INPUT
java -cp lib/cloudsim-2.1.1.jar:dist/cloudworkflowsimulator.jar cws.core.experiment.Experiment $INPUT