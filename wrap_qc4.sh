#!/bin/bash

loc_exp=(
ca547
ca548
ca587
)

for exp in ${loc_exp[@]}; do
  ./pre_qc4.sh $exp
  #./run_qc4.sh $exp
  #break
done
exit
