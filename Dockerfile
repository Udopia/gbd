FROM python:slim
COPY . /
RUN apt-get update -y \
  && apt-get install -y nginx awstats logrotate mailutils- mailutils-common- exim4-base- exim4-config- exim4-daemon-light-\
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
  && cp configs/nginx_proxy.conf /etc/nginx/nginx.conf \
  && mkdir -p /internal_proxy/www \
  && chown www-data:root /internal_proxy \
  && sed -i 's/LogFile=.*$/LogFile=\/internal_proxy\/access.log/' /etc/awstats/awstats.conf \
  && sed -i 's/DirData=.*$/DirData=\/internal_proxy/' /etc/awstats/awstats.conf \
  && sed -i 's/DirIcons=.*$/DirIcons=\/stats\/icon/' /etc/awstats/awstats.conf \
  && sed -i 's/\/var\/log\/nginx\/\*\.log/\/internal_proxy\/access\.log/' /etc/logrotate.d/nginx \
  && sed -i 's/.*invoke-rc\.d\ nginx.*$/\t\tnginx -s reload/' /etc/logrotate.d/nginx \
  && sed -i 's/.*create.*$/\tcreate 0644 root root/' /etc/logrotate.d/nginx
RUN pip install setuptools flask flask_limiter tatsu waitress pandas numpy
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8
EXPOSE 80
WORKDIR .
CMD ["./entrypoint.sh"]
