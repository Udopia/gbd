#!/bin/bash

if [ $# -lt 1 ]; then
	echo "Usage: $0 [ nginx | gbd | ... ]"
	exit 0
fi

if [ ! -e Dockerfile.$1 ]; then
	echo "Dockerfile.$1 not found"
	exit 1
fi

docker build --no-cache -t my$1 -f Dockerfile.$1 .
