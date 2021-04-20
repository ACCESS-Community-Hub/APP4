#!/bin/bash

loc_exp=(
ca199
cd152
cd153
)

for exp in ${loc_exp[@]}; do
  ./run_app4.sh $exp
done
exit

