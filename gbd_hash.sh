#!/bin/bash

if [ $# -lt 1 ]; then 
	echo "Usage: $0 [cnf-file]"
fi

file=$1
command="cat"

if [[ $file == *.gz ]]; then 
  command="zcat"
fi

$command $file | grep -v '^c\|^p' | tr '\r\n' ' ' | sed 's/\s\+/ /g;s/^\s//g;s/\s$//g' | sed 's/[^0]$/& 0/' | md5sum
 


