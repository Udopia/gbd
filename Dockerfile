FROM python:3.8-alpine
COPY . /
RUN pip install setuptools flask flask_limiter tatsu waitress
RUN apt-get -y install python3-numpy python3-pandas
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8
EXPOSE 5000
WORKDIR .
ENTRYPOINT ["python", "./server.py"]
