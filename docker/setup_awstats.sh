#!/bin/bash
set -euo pipefail

# Host root; override by exporting GBD_ROOT. Must match ${GBD_ROOT:-/home/iser} in docker-compose.yml.
secret_file="${GBD_ROOT:-/home/iser}/nginx/secrets/awstats.htpasswd"

if [ $# -ne 1 ]; then
	echo "Usage: $0 <username>"
	echo "Creates $secret_file for the AWStats basic-auth,"
	echo "mounted into the nginx container as the docker secret 'awstats_htpasswd'."
	exit 1
fi

username="$1"

if ! command -v htpasswd >/dev/null 2>&1; then
	echo "htpasswd not found. Install apache2-utils (Debian/Ubuntu) or httpd-tools (RHEL/Alpine)." >&2
	exit 1
fi

secret_dir="$(dirname "$secret_file")"
mkdir -p "$secret_dir"

# -c (re)creates the file; default apr1/MD5 hashing is natively supported by nginx on alpine.
# Password is prompted for, so it never lands in shell history or process arguments.
htpasswd -c "$secret_file" "$username"

echo "Wrote $secret_file"
