#!/bin/bash

set -e
trap "exit" INT

#simulation parameters
APPLICATION=GENOME
INDIR=SyntheticWorkflows/$APPLICATION
ALGORITHM=SPSS
DEADLINES=5
BUDGETS=5
DISTR=pareto_unsorted
STORAGE_MNG=void
ENSEMBLE_SIZE=10
SCALING_FACTOR=1.0
STORAGE_CACHE=void
SEED=123124
ENABLE_LOGGING=true
LOG_TO_STDOUT=false
VM_TYPE_SELECTION=viable
LOGFILE_BASE=sim_out_log

#script parameters
SIM_DIR=`date +%Y-%m-%d:%H:%M:%S`
RUNS=3
EXP_SCORES=()
PREPROCESSED_LOGS_DIR="preprocessed"
VISUALISATION_RUBY_SCRIPT=visualize_exp_score.rb
AVG_SCORE_EXTRACTOR=avg_score_multiple_runs.rb
AVG_CSV=avg_for_$ALGORITHM.csv
MIN_DEADLINE_ROW=2
DEADLINE_COL=7
BUDGETS_COL=6
SCORE_COL=10

for (( i=1; i<=$RUNS; i++ ))
do
    #parameters that should be unique for every simulation run
    LOGFILE=${LOGFILE_BASE}_${i}.csv

    #simulation
    ant run-sim-locally -Dalgo=$ALGORITHM -Dapp=$APPLICATION -Dindir=$INDIR -Dout=$LOGFILE -Ddistr=$DISTR -Dstoragemng=$STORAGE_MNG -Densemblesize=$ENSEMBLE_SIZE -Dscalingfactor=$SCALING_FACTOR -Dseed=$SEED -Dstoragecache=$STORAGE_CACHE -Denablelog=$ENABLE_LOGGING -Dstdoutlog=$LOG_TO_STDOUT -Dbudgets=$BUDGETS -Ddeadlines=$DEADLINES -Dvmtypeselection=$VM_TYPE_SELECTION

    #preprocessing logs
    mkdir -p $PREPROCESSED_LOGS_DIR
    for logfile in *.log; do
        PATH_TO_LOGFILE="./../"${logfile}
        PATH_TO_PREPROCESSED_LOG_FILE="./../"${PREPROCESSED_LOGS_DIR}"/"${logfile::${#logfile}-4}"_preprocessed.log"
        echo "Preprocessing "${logfile}
        cd scripts
        python -m log_parser.parse_experiment_log $PATH_TO_LOGFILE $PATH_TO_PREPROCESSED_LOG_FILE
        cd ..
    done

    #validating parsed logs
    for preprocessed_logfile in ${PREPROCESSED_LOGS_DIR}/*_preprocessed.log; do
        PATH_TO_PREPROCESSED_LOGFILE="./../"${preprocessed_logfile}
        echo "Validating "${preprocessed_logfile}
        cd scripts
        set +e
        python -m validation.experiment_validator $PATH_TO_PREPROCESSED_LOGFILE
        set -e
        cd ..
    done

    #visualising scheduling process
    for preprocessed_logfile in ${PREPROCESSED_LOGS_DIR}/*_preprocessed.log; do
        TMP="../../"${preprocessed_logfile}
        cd scripts/visualisation
        echo `pwd`
        TMP2="../"$TMP
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
    ruby $VISUALISATION_RUBY_SCRIPT -c $LOGFILE -n $BUDGETS -r $MIN_DEADLINE_ROW -d $DEADLINE_COL -b $BUDGETS_COL -s $SCORE_COL

    #moving created files to sorted directories for cleaner view
    mkdir -p $SIM_DIR/$i/logs
    mkdir -p $SIM_DIR/$i/csv
    mkdir -p $SIM_DIR/$i/scores

    mv $PREPROCESSED_LOGS_DIR/ $SIM_DIR/$i/logs
    mv *csv $SIM_DIR/$i/csv
    mv *log $SIM_DIR/$i/logs
    mv *png $SIM_DIR/$i/scores

done

#preparing avg data for visualizing
ruby $AVG_SCORE_EXTRACTOR -i $SIM_DIR -l $LOGFILE_BASE -r $RUNS -n $BUDGETS -d $DEADLINE_COL -b $BUDGETS_COL -s $SCORE_COL -o $AVG_CSV

#visualizing avg score for every budget
DEADLINE_COL=2
BUDGETS_COL=1
SCORE_COL=3
ruby $VISUALISATION_RUBY_SCRIPT -c $AVG_CSV -n $BUDGETS -r $MIN_DEADLINE_ROW -d $DEADLINE_COL -b $BUDGETS_COL -s $SCORE_COL
AVG_SCORES_DIR=$SIM_DIR/avg/scores
mkdir -p $AVG_SCORES_DIR
mv *png $AVG_SCORES_DIR
mv $AVG_CSV $AVG_SCORES_DIR
