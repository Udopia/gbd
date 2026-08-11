#!/bin/bash
# Certbot deploy hook: copy the renewed certificate to where the nginx container
# reads it, then reload nginx with zero downtime. Runs automatically after a
# successful renewal once symlinked into /etc/letsencrypt/renewal-hooks/deploy/.
set -euo pipefail

domain="benchmark-database.de"
ssl_dir="${GBD_ROOT:-/home/iser}/nginx/ssl"
nginx_container="gbd-nginx"

# As a global deploy hook this runs for every lineage; act only on ours.
if [ -n "${RENEWED_LINEAGE:-}" ] && [ "$(basename "$RENEWED_LINEAGE")" != "$domain" ]; then
	exit 0
fi

src="/etc/letsencrypt/live/$domain"
install -D -m 644 "$src/fullchain.pem" "$ssl_dir/fullchain.pem"
install -D -m 600 "$src/privkey.pem"   "$ssl_dir/privkey.pem"

# SIGHUP via the nginx binary makes the running master re-read the new certs.
docker exec "$nginx_container" nginx -s reload \
	|| echo "deploy-cert: could not reload $nginx_container (not running?); certs are in place for next start" >&2
