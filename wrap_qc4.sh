#!/bin/bash

loc_exp=(
PI-01
SSP-126-ext-05
SSP-126-ext-06
SSP-126-ext-07
SSP-126-ext-08
SSP-126-ext-09
SSP-126-ext-10
SSP-126-ext-11
SSP-126-ext-12
SSP-126-ext-13
SSP-126-ext-14
)

for exp in ${loc_exp[@]}; do
  ./pre_qc4.sh $exp
  #./run_qc4.sh $exp
done
