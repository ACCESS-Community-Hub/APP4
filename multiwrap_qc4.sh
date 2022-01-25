#!/bin/bash
set -a 

loc_exp=(
bj594
)

mode=production
for exp in ${loc_exp[@]}; do
  ./check_app4.sh $exp
  #./production_qc4.sh $exp
  #break
done
exit
