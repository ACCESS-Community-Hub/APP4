#!/bin/bash

echo "WARNING: This script will delete the \${MAIN_DIR}/APP_job_files directories for all experiments listed in input_files/multi_exp_list.csv!"
echo "Press ENTER to continue..."
read cont

while IFS= read -r expt
do
  echo ""
  echo "-=-=-=-=--- $expt:"
  if [[ $expt == "#"* ]]; then
    echo "  skipping"
  else
    ./run_app4.sh $expt
  fi
  #break
done < input_files/multi_exp_list.csv
