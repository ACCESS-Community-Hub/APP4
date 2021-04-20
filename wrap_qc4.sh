#!/bin/bash

loc_exp=(
ca199
cd152
cd153
)

for exp in ${loc_exp[@]}; do
  #./pre_qc4.sh $exp
  ./run_qc4.sh $exp
done
exit
