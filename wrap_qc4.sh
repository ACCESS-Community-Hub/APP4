#!/bin/bash

loc_exp=(
ca548
)

for exp in ${loc_exp[@]}; do
  ./pre_qc4.sh $exp
  #./run_qc4.sh $exp
  #break
done
exit
