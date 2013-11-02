#!/bin/bash
# Filters the given CSV file leaving only selected columns.
# Example: you have a file with 10 columns named A,B,C,...,X and you call this script with
# A,C,F params, the output CSV file will contain only those columns.

if [ "$#" -ne 2 ]; then
  echo -e "Usage:\n\t$0 [column list] [simulation output CSV file]";
  exit 1;
fi

COLS=$1;

echo $COLS;
csvtool namedcol $COLS $2;
