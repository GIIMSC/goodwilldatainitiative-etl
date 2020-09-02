# This Dockerfile can be used for local development. 
# It provides a way to inject the `goodwilldatainitiative-etl` package into an Airflow container.
#
# $ docker build -t docker-airflow:latest .
FROM puckel/docker-airflow:1.10.9

USER root

ADD . goodwilldatainitiative-etl

RUN apt-get update \
    && apt-get install -y git \
    && pip install --upgrade pip && GIT_SSL_NO_VERIFY=True pip install -e goodwilldatainitiative-etl