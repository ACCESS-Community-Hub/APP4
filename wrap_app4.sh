#!/bin/bash

loc_exp=(
ca547
ca548
ca587
)

for exp in ${loc_exp[@]}; do
  ./run_app4.sh $exp
  #break
done
exit


ca547
ca548
ca587
