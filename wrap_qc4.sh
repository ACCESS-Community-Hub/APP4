#!/bin/bash

loc_exp=(
SSP-585-15
SSP-585-16
SSP-585-17
SSP-585-18
SSP-585-19
SSP-585-20
SSP-585-21
SSP-585-22
SSP-585-23
SSP-585-24
)

for exp in ${loc_exp[@]}; do
  #./pre_qc4.sh $exp
  ./run_qc4.sh $exp
  #break
done
exit
