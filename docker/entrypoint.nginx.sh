#!/bin/bash

# Configures nginx and starts it
sed -i "s/__VIRTUAL_HOST__/$VIRTUAL_HOST/g" /etc/nginx/nginx.conf

nginx


# Installs awstats basic-auth file from the mounted docker secret
install -m 0444 /run/secrets/awstats_htpasswd /awstats/htpasswd


# Ensures the output dir exists on the persisted (initially empty) /awstats volume
mkdir -p /awstats/www


# Configures cron and starts it
/usr/bin/awstats_buildstaticpages.pl -config=$VIRTUAL_HOST -update -dir=/awstats/www
printf "#!/bin/bash\n/usr/bin/awstats_buildstaticpages.pl -config=$VIRTUAL_HOST -update -dir=/awstats/www" > /etc/periodic/15min/awstats
chmod +x /etc/periodic/15min/awstats
ln -fs /usr/share/zoneinfo/Europe/Berlin /etc/localtime

crond -f -l 8