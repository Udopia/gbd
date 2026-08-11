#!/bin/bash
# Rebuild both images from this directory and (re)start the stack.
#
# Run on the server after `git pull` (and after publishing a new gbd-tools
# release, since the gbd image installs gbd-tools from PyPI). Data under
# GBD_ROOT (databases, AWStats, certificates) persists across rebuilds.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

root="${GBD_ROOT:-/home/iser}"
secret="$root/nginx/secrets/awstats.htpasswd"
if [ ! -e "$secret" ]; then
	echo "Missing $secret — run ./setup_awstats.sh <username> first." >&2
	exit 1
fi

./build.sh gbd
./build.sh nginx

docker compose up -d
docker image prune -f

echo "Stack is up:"
docker compose ps
