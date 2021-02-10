#!/bin/bash

loc_exp=(
SSP-126-ext-07
SSP-126-ext-09
)

for exp in ${loc_exp[@]}; do
  ./run_app4.sh $exp
done
