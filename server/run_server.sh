#!/usr/bin/env bash
if [ $# -lt 1 ]; then
  echo "Usage: $0 [database]"
  exit 0
fi
export GBD_DB_SERVER=$1
FLASK_APP=server.py flask run --host=0.0.0.0
