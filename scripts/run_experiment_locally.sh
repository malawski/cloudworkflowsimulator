#!/bin/bash                                                                                                                                                                 

if [ $# -lt 1 ]; then
        echo "Usage: $0 EXPERIMENT_PROPERTIES_DIR"
        exit 1
fi


ant 
INPUT_DIR=$1

INPUTS=$( ls -1 $INPUT_DIR/input*.properties)

if [ $# -eq 2 ]; then
        INPUTS=$( ls -1 $INPUT_DIR/input*.properties | grep $2)
fi



for INPUT in $INPUTS ; do 
        echo java -cp lib/cloudsim-2.1.1.jar:dist/cloudworkflowsimulator.jar cws.core.experiment.Experiment $INPUT
        java -cp lib/cloudsim-2.1.1.jar:dist/cloudworkflowsimulator.jar cws.core.experiment.Experiment $INPUT
done

exit 0
