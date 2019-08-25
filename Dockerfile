FROM ubuntu:18.04
COPY . /
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
 chmod +x server/run_server.sh \
 && rm -rf /var/lib/apt/lists/*
ENV LC_ALL=C.UTF-8 \
  LANG=C.UTF-8
EXPOSE 5000
WORKDIR /server
CMD ./run_server.sh
