#!/bin/bash

loc_exp=(
PI-1pct-rev-01
PI-rev-01
)

for exp in ${loc_exp[@]}; do
  ./run_app4.sh $exp
  #break
done
exit


ca547
ca548
ca587
