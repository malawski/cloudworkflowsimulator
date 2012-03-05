#!/bin/bash
# create SGE array job

if [ $# -lt 1 ]; then
	echo "Usage: $0 INPUT_DIR"
	exit 1
fi


INPUT_LIST=$1


NUM_INPUTS=$(cat $INPUT_LIST | wc -l)

# try to add SMP flag to run on 4 cores instead of 8
echo qsub -pe smp 2 -q short -t 1-$NUM_INPUTS /afs/crc.nd.edu/user/m/mmalawsk/cloudworkflowsimulator/rerun_experiment.sh $INPUT_LIST


