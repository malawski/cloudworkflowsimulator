#!/bin/bash
# create SGE array job

if [ $# -lt 1 ]; then
	echo "Usage: $0 INPUT_DIR"
	exit 1
fi


INPUT_DIR=$1


NUM_INPUTS=$(ls -1 $INPUT_DIR/input*.properties | wc -l)

# SMP flag to run on 4 cores instead of 8
echo qsub -pe smp 2 -q short -t 1-$NUM_INPUTS /afs/crc.nd.edu/user/m/mmalawsk/cloudworkflowsimulator/run_experiment.sh $INPUT_DIR


