#!/bin/bash
set -a 

loc_exp=(
SSP-245-40
)

MODE=production
for exp in ${loc_exp[@]}; do
  ./production_app4.sh $exp
  #./check_app4.sh $exp
  #./production_qc4.sh $exp
  #break
done
exit

