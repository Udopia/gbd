#!/usr/bin/env bash
if [ $# -lt 1 ]; then
  if [ -z ${GBD_DB} ]; then
    echo "Usage: $0 [database] or set environment variable GBD_DB"
    exit 0
  else
    echo "Using fallback: ${GBD_DB}"
    python3 server.py -d ${GBD_DB}
  fi
else
    echo "Using database: $1"
  python3 server.py -d $1
fi
