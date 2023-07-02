#!/bin/bash
set -a 

loc_exp=(
SSP-534-lu-05
)

MODE=production
for exp in ${loc_exp[@]}; do
  #./check_app4.sh $exp
  ./production_qc4.sh $exp
  #break
done
exit
