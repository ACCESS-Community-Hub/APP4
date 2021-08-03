#!/bin/bash

loc_exp=(
cg320
cg321
cg322
cg323
)

for exp in ${loc_exp[@]}; do
  #./pre_qc4.sh $exp
  ./run_qc4.sh $exp
  #break
done
exit
