#!/bin/bash

trap "exit" INT

#script parameters
PREPROCCESSED_LOGS_DIR="preprocessed_logs"
VISUALISATION_DIR="visualised_results"
VISUALISATION_RUBY_SCRIPT=visualize_exp_score.rb

#simulation parameters
APPLICATION=GENOME
INDIR=SyntheticWorkflows/$APPLICATION
ALGORITHM=DPDS
DEADLINES=3
BUDGETS=3
DISTR=pareto_unsorted
STORAGE_MNG=void
ENSEMBLE_SIZE=10
SCALING_FACTOR=1.0
STORAGE_CACHE=void
SEED=123123
ENABLE_LOGGING=true
LOG_TO_STDOUT=false
RUNS=1
LOGFILE=sim_out_log.csv
VM_TYPE_SELECTION=viable

#simulation
ant run-sim-locally -Dalgo=$ALGORITHM -Dapp=$APPLICATION -Dindir=$INDIR -Dout=$LOGFILE -Ddistr=$DISTR -Dstoragemng=$STORAGE_MNG -Densemblesize=$ENSEMBLE_SIZE -Dscalingfactor=$SCALING_FACTOR -Dseed=$SEED -Dstoragecache=$STORAGE_CACHE -Denablelog=$ENABLE_LOGGING -Dstdoutlog=$LOG_TO_STDOUT -Dbudgets=$BUDGETS -Ddeadlines=$DEADLINES -Dvmtypeselection=$VM_TYPE_SELECTION

#preprocessing logs
mkdir -p $PREPROCCESSED_LOGS_DIR
for logfile in *.log; do
    PATH_TO_LOGFILE="./../"${logfile}
    PATH_TO_PREPROCESSED_LOG_FILE="./../"${PREPROCCESSED_LOGS_DIR}"/"${logfile::${#logfile}-4}"_preprocessed.log"
    echo "Preprocessing "${logfile}
    cd scripts
    python -m log_parser.parse_experiment_log $PATH_TO_LOGFILE $PATH_TO_PREPROCESSED_LOG_FILE
    cd ..
done

#validating parsed logs
for preprocessed_logfile in ${PREPROCCESSED_LOGS_DIR}/*_preprocessed.log; do
    PATH_TO_PREPROCESSED_LOGFILE="./../"${preprocessed_logfile}
    echo "Validating "${preprocessed_logfile}
    cd scripts
    python -m validation.experiment_validator $PATH_TO_PREPROCESSED_LOGFILE
    cd ..
done

#visualising scheduling process
for preprocessed_logfile in ${PREPROCCESSED_LOGS_DIR}/*_preprocessed.log; do
    TMP="../../"${preprocessed_logfile}
    cd scripts/visualisation
    echo `pwd`
    echo "Drawing results graph for "${TMP}
    ruby plot_gantt.rb results $TMP ${TMP}_results_graph
    echo "Drawing workflow graph for "${TMP}
    ruby plot_gantt.rb workflow $TMP ${TMP}_workflow_graph
    echo "Drawing storage graph for "${TMP}
    ruby plot_gantt.rb storage $TMP ${TMP}_storage_graph
    cd ../..
done

#visualising score for every budget
echo "Rendering graphs with exp. score and normalized deadline"
ruby $VISUALISATION_RUBY_SCRIPT $LOGFILE $BUDGETS
