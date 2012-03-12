#!/bin/bash


if [ $# -lt 1 ]; then
	echo "Usage: $0 INPUT_DIR"
	exit 1
fi

INPUT_DIR=$1

SUBMIT_SCRIPT=submit-${INPUT_DIR}.sh
#SUBMIT_SCRIPT=scripts/submit_experiments.sh

ant 
./scripts/submit_experiments.sh $INPUT_DIR > $SUBMIT_SCRIPT
chmod a+x $SUBMIT_SCRIPT
cp -r ${INPUT_DIR} /afs/crc.nd.edu/user/m/mmalawsk/cloudworkflowsimulator/
cp $SUBMIT_SCRIPT /afs/crc.nd.edu/user/m/mmalawsk/cloudworkflowsimulator/
cp dist/cloudworkflowsimulator.jar /afs/crc.nd.edu/user/m/mmalawsk/cloudworkflowsimulator/dist/

echo ./$SUBMIT_SCRIPT