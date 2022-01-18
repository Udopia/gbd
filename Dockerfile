FROM python:slim
COPY . /
RUN pip install setuptools flask flask_limiter tatsu waitress pandas numpy
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8
EXPOSE 5000
WORKDIR .
ENTRYPOINT ["python", "./server.py"]
