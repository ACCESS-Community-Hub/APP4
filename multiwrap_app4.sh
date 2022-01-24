#!/bin/bash

loc_exp=(
bj594
)

for exp in ${loc_exp[@]}; do
  ./production_app4.sh $exp
  #break
done
exit

