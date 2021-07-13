#!/bin/bash

loc_exp=(
ce948
cf316
cf317
cf318
)

for exp in ${loc_exp[@]}; do
  #./pre_qc4.sh $exp
  ./run_qc4.sh $exp
  #break
done
exit
