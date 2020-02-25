FROM ubuntu:18.04
COPY . /
RUN apt-get update && apt-get install -y git python3 python3-pip && \
 pip3 install global-benchmark-database-tool && \
 rm -rf /var/lib/apt/lists/*
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8 GBD_DB="/raid/iser/git/gbd-data/iser.db"
EXPOSE 5000
WORKDIR .
ENTRYPOINT ["gbd-server"]
