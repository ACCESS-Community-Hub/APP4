#!/bin/bash

export MULTI_EXP=1

echo "Press ENTER to continue..."
read cont

while IFS= read -r expt
do
  echo ""
  echo "-=-=-=-=--- $expt:"
  if [[ $expt == "#"* ]]; then
    echo "  skipping"
  else
    #./prepublish_check.sh $expt
    ./prepublish_check_multi.sh $expt
  fi
done < input_files/multi_exp_list.csv
