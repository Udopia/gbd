#!/bin/bash
# Rebuild both images from this directory and (re)start the stack.
#
# Run on the server after `git pull` (and after publishing a new gbd-tools
# release, since the gbd image installs gbd-tools from PyPI). Data under
# GBD_ROOT (databases, AWStats, certificates) persists across rebuilds.
#
# The previously deployed images are tagged ":prev" before rebuilding, and a
# health check runs after startup, so a bad deploy is caught and can be rolled
# back instantly.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

root="${GBD_ROOT:-/home/iser}"
secret="$root/nginx/secrets/awstats.htpasswd"
if [ ! -e "$secret" ]; then
	echo "Missing $secret — run ./setup_awstats.sh <username> first." >&2
	exit 1
fi

# Keep the currently-deployed images tagged :prev for instant rollback.
for img in mygbd mynginx; do
	docker image inspect "$img" >/dev/null 2>&1 && docker tag "$img" "$img:prev"
done

./build.sh gbd
./build.sh nginx

docker compose up -d

# Health check: nginx up and proxying to gbd (expect 200 from the app).
echo "Waiting for the stack to serve..."
code=000
for _ in $(seq 1 20); do
	code=$(curl -sk -o /dev/null -w '%{http_code}' --max-time 5 \
		-H 'Host: benchmark-database.de' https://localhost/ || true)
	[ "$code" = "200" ] && break
	sleep 2
done

if [ "$code" = "200" ]; then
	docker image prune -f
	echo "Stack is up and serving (HTTP $code):"
	docker compose ps
else
	echo "HEALTH CHECK FAILED (last status: $code)." >&2
	docker logs --tail=20 gbd-nginx >&2 || true
	echo "Roll back with:" >&2
	echo "  docker tag mygbd:prev mygbd && docker tag mynginx:prev mynginx && docker compose up -d" >&2
	exit 1
fi
