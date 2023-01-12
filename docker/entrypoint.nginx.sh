#!/bin/bash

# Configures nginx and starts it
sed -i "s/__VIRTUAL_HOST__/$VIRTUAL_HOST/g" /etc/nginx/nginx.conf

nginx


# Configures awstats user and password
htpasswd -cb /awstats/htpasswd $AWSTATS_USER $AWSTATS_PASS


# Configures cron and starts it
/usr/bin/awstats_buildstaticpages.pl -config=$VIRTUAL_HOST -update -dir=/awstats/www
printf "#!/bin/bash\n/usr/bin/awstats_buildstaticpages.pl -config=$VIRTUAL_HOST -update -dir=/awstats/www" > /etc/periodic/15min/awstats
chmod +x /etc/periodic/15min/awstats
ln -fs /usr/share/zoneinfo/Europe/Berlin /etc/localtime

crond -f -l 8