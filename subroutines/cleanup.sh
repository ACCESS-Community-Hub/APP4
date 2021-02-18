#!/bin/bash

OUT_DIR=$1

if [ -z $OUT_DIR ]; then
  echo 'no app_dir; using defaults'
  rm -f *.pyc
  rm -f subroutines/*.pyc
  rm -f *.o[0-9]*
  rm -f sys
else
  echo -e 'preparing job_files directory...'
  if [ -d $OUT_DIR ]; then
    echo "output directory '${OUT_DIR}' exists. press enter to delete and continue. exit manually otherwise."
    read cont
  fi
  rm -f *.pyc
  rm -f subroutines/*.pyc
  rm -f sys
  rm -rf $OUT_DIR
  mkdir -p $SUCCESS_LISTS
  mkdir -p $VARIABLE_MAPS
  mkdir -p $CMOR_LOGS
  mkdir -p $VAR_LOGS
fi
