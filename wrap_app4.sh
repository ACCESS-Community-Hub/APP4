#!/bin/bash

loc_exp=(
SSP-370-35
SSP-370-36
SSP-370-37
SSP-370-38
SSP-370-39
SSP-370-40
SSP-370-41
SSP-370-42
SSP-370-43
SSP-370-44
)

for exp in ${loc_exp[@]}; do
  ./run_app4.sh $exp
done
exit

