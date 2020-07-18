#!/bin/bash

if [ $# -lt 1 ]; then 
	echo "Usage: $0 [cnf-file]"
fi

file=$1
command="cat"

if [[ $file == *.gz ]]; then 
  command="zcat"
elif [[ $file == *.bz2 ]]; then
  command="bzcat"
elif [[ $file == *.xz ]]; then
  command="xzcat"
fi

$command $file | sed 's/[[:space:]]+/ /g;s/^[[:space:]]//g;s/[[:space:]]$//g' | grep -v '^c\|^p' | tr -s '\n\r' ' ' | md5sum

# this adds trainling zero if missing (but buffer-overruns for large files due to missing line-breaks after above tr-command): sed 's/[^0]$/& 0/' 
 


