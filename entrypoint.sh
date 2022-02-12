#!/bin/bash

# add host to config files
sed -i 's/SiteDomain=.*$/SiteDomain=\"'$VIRTUAL_HOST'\"/' /etc/awstats/awstats.conf
sed -i 's/HostAliases=.*$/HostAliases=\"'$VIRTUAL_HOST'\"/' /etc/awstats/awstats.conf
sed -i 's/index localhost.*$/index awstats\.'$VIRTUAL_HOST'\.html index\.html;/' /etc/nginx/nginx.conf
sed -i '/^\tprerotate.*/a \\t\t\/usr\/share\/awstats\/tools\/awstats_buildstaticpages\.pl -config='$VIRTUAL_HOST' -update -dir=\/internal_proxy\/www' /etc/logrotate.d/nginx
 echo -e '*/10 * * * *\troot\t/usr/share/awstats/tools/awstats_buildstaticpages.pl -config='$VIRTUAL_HOST' -update -dir=/internal_proxy/www' >> /etc/crontab
ln -fs /usr/share/zoneinfo/Europe/Berlin /etc/localtime
chown www-data:root /internal_proxy

# start proxy, cron and gbd
service cron start
nginx
python ./server.py
