FROM spark:3.5.7-scala2.12-java11-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

COPY requirements-pipeline.txt .
COPY ./spark/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"

RUN pip install -r requirements-pipeline.txt

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}