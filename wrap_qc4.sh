#!/bin/bash

loc_exp=(
HI-10-re1
SSP-126-10-re1
SSP-245-10-re1
SSP-370-10-re1
SSP-585-10-re1
)

for exp in ${loc_exp[@]}; do
  #./pre_qc4.sh $exp
  ./run_qc4.sh $exp
  #break
done
exit
