#!/bin/bash

loc_exp=(
PI-1pct-rev-01
PI-rev-01
)

for exp in ${loc_exp[@]}; do
  #./pre_qc4.sh $exp
  ./run_qc4.sh $exp
  #break
done
exit
