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
secret_dir="$(dirname "$secret_file")"
mkdir -p "$secret_dir"

# Both branches write an apr1/MD5 hash, which nginx supports natively. The
# password is prompted for, so it never lands in shell history or process args.
if command -v htpasswd >/dev/null 2>&1; then
	htpasswd -c "$secret_file" "$username"
elif command -v openssl >/dev/null 2>&1; then
	# Fallback for hosts without apache2-utils (e.g. Debian buster, now EOL).
	read -rsp "New password: " pw; echo
	read -rsp "Re-type new password: " pw2; echo
	[ "$pw" = "$pw2" ] || { echo "passwords do not match" >&2; exit 1; }
	hash="$(printf '%s\n' "$pw" | openssl passwd -apr1 -stdin)"
	printf '%s:%s\n' "$username" "$hash" > "$secret_file"
	unset pw pw2 hash
else
	echo "Need htpasswd (apache2-utils) or openssl to create $secret_file." >&2
	exit 1
fi

echo "Wrote $secret_file"
