#!/bin/bash
echo "updating version date...${version}"
scriptpath="$( cd "$(dirname "$0")" ; pwd -P )"
cd $scriptpath

jsonlist=(`ls ${EXP_TO_PROCESS}.json`)
dateline="    \"version\":                      \"v${version}\","

for json in ${jsonlist[@]}
do
  vline=$(grep '\"version\":' $json)
  #echo $vline
  #echo $dateline
  sed -i "s/${vline}/${dateline}/g" $json
done
