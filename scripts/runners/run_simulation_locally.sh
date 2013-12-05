#!/bin/bash
# TODO(bryk): Comments.

if [ $# -lt 1 ]; then
        echo "Usage: $0 '<simulation args>' '<optional log prefix to generate graphs for>'"
        exit 1
fi

ARGS=$1
LOG_PREFIX=$2
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


(set -x && java -cp "${DIR}/../../lib/*:${DIR}/../../bin/*:${DIR}/../../bin/" cws.core.algorithms.TestRun $ARGS)

if [ $LOG_PREFIX ] ; then
  for LOG in `ls ${LOG_PREFIX}*`; do
    LOG=`readlink -f "$LOG"`
    echo "Processing ${LOG}"
    (
      cd $DIR/..
      python -m log_parser.parse_experiment_log "${LOG}" "${LOG}.postlog"
      (
        cd visualisation
        ruby plot_gantt.rb results "${LOG}.postlog" "${LOG}.results.png"
        ruby plot_gantt.rb workflow "${LOG}.postlog" "${LOG}.workflow.png"
        ruby plot_gantt.rb storage "${LOG}.postlog" "${LOG}.storage.png"
        ruby plot_storage.rb number "${LOG}.postlog" "${LOG}.storage.number.png"
        ruby plot_storage.rb speed "${LOG}.postlog" "${LOG}.storage.speed.png"
      )
    )
  done;
fi

