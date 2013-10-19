#!/bin/bash
# Filters the given CSV file leaving only selected columns.
# TODO(bryk): Add more comments.

if [ "$#" -ne 2 ]; then
  echo -e "Usage:\n\t$0 [column list] [simulation output CSV file]";
  exit 1;
fi

COLS=$1;

echo $COLS;
csvtool namedcol $COLS $2;

