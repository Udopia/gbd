FROM ubuntu:18.04
RUN apt-get update && apt-get install -y \
  git \
  python3 \
  python3-pip && \
 pip3 install \
  flask_limiter \
  global-benchmark-database-tool \
  tatsu \
  setuptools \
  flask && \
 chmod +x run_server.sh
ENV LC_ALL=C.UTF-8 \
  LANG=C.UTF-8
WORKDIR /gbd
EXPOSE 5000
CMD ./run_server.sh
