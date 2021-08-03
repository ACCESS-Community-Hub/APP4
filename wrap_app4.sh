#!/bin/bash

loc_exp=(
cg320
cg321
cg322
cg323
)

for exp in ${loc_exp[@]}; do
  ./run_app4.sh $exp
done
exit
