#!/bin/bash
# One-time host preparation for the gbd docker stack.
#
# Creates the directory skeleton the compose bind mounts expect, drops a
# self-signed placeholder certificate so nginx can boot before the real
# Let's Encrypt certificate is issued, and checks prerequisites.
set -euo pipefail

root="${GBD_ROOT:-/home/iser}"
domain="benchmark-database.de"
ssl="$root/nginx/ssl"

echo "Preparing host layout under GBD_ROOT=$root"
mkdir -p \
	"$ssl/bot" \
	"$root/nginx/awstats" \
	"$root/nginx/secrets" \
	"$root/gbd" \
	"$root/logs"

# Placeholder cert so nginx's 443 server block can start before certbot runs.
if [ ! -e "$ssl/fullchain.pem" ] || [ ! -e "$ssl/privkey.pem" ]; then
	if command -v openssl >/dev/null 2>&1; then
		echo "Creating self-signed placeholder certificate (replace it via certbot)..."
		openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
			-keyout "$ssl/privkey.pem" -out "$ssl/fullchain.pem" \
			-subj "/CN=$domain"
		chmod 600 "$ssl/privkey.pem"
	else
		echo "NOTE: openssl not found; provide $ssl/{fullchain,privkey}.pem yourself before starting nginx." >&2
	fi
fi

# Prerequisite checks.
missing=0
command -v docker >/dev/null 2>&1 || { echo "MISSING: docker"; missing=1; }
docker compose version >/dev/null 2>&1 || { echo "MISSING: docker compose plugin"; missing=1; }
command -v htpasswd >/dev/null 2>&1 || command -v openssl >/dev/null 2>&1 \
	|| echo "NOTE: install apache2-utils (htpasswd) or openssl for ./setup_awstats.sh"
command -v certbot  >/dev/null 2>&1 || echo "NOTE: install certbot on the host for TLS issuance/renewal"

[ -e "$root/nginx/secrets/awstats.htpasswd" ] \
	|| echo "NEXT: ./setup_awstats.sh <username>   (creates the AWStats basic-auth secret)"
echo "NEXT: copy your GBD *.db files into $root/gbd   (mounted read-only into the gbd container)"

if [ "$missing" -ne 0 ]; then
	echo "Install the MISSING prerequisites above, then re-run." >&2
	exit 1
fi
echo "Host preparation complete."
