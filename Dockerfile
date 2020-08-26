FROM puckel/docker-airflow:1.10.2

USER root

ADD . goodwilldatainitiative-etl

RUN apt-get update \
    && apt-get install -y git \
    && pip install --upgrade pip && GIT_SSL_NO_VERIFY=True pip install -e goodwilldatainitiative-etl