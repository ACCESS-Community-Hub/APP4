#!/bin/bash

loc_exp=(
bj594
)

for exp in ${loc_exp[@]}; do
  ./run_app4.sh $exp
done
exit

