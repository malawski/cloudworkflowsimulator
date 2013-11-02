#!/bin/bash
# This script can be used for lightweight analysis of transfer outputs of a simulation.
# It runs graphical console tool for displaying CSV files, thus is far more faster than Excel or OpenOffice.
# Furthermore, it can be run in a console-only environment.
# TODO(bryk): Add more comments.

COLSFILE='transter_outputs_columns.lst';

if [ "$#" -ne 1 ]; then
  echo -e "Usage:\n\t$0 [simulation output CSV file]";
  exit 1;
fi

hash csvtool 2> /dev/null || {
  echo >&2 "csvtool program is required to run this script. You can install it from your program repository.";
  exit 1;
}

hash tabview 2> /dev/null || {
  echo >&2 "tabview program is required to run this script. You can install it from github repository: https://github.com/firecat53/tabview";
  exit 1;
}

TMPFILE=mktemp;

cat $(COLSFILE) > $(TMPFILE) && csvtool namedcol `cat $(COLSFILE)` $1 | awk -F ',' '\
{ \
  hum[1024**5] = "PB"; \
  hum[1024**4] = "TB"; \
  hum[1024**3] = "GB"; \
  hum[1024**2] = "MB"; \
  hum[1024] = "KB"; \
  for (i = 1; i <= 5; i++) { \
    sum = $i; \
    for (x = 1024**5; x >= 1024; x /= 1024) { \
      if (sum >= x) { \
        printf "%.2f%s,", sum/x, hum[x]; \
        break; \
      } \
    } \
  } \
  for (i = 6; i < NF; i++) { \
    printf "%s,", $i; \
  } \
  printf "%s\n", $NF; \
}' >> $(TMPFILE) && tabview $(TMPFILE);

rm $(TMPFILE);


