FROM apache/spark:4.1.0-scala2.13-java21-ubuntu

USER root

RUN set -ex; \
    apt-get upgrade; \
    apt-get update; \
    apt install -y software-properties-common; \
    add-apt-repository ppa:deadsnakes/ppa; \
    apt-get update; \
    apt-cache policy python3.14; \
    apt-get install -y python3.14 python3-pip; \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 10; \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.14 14; \
    update-alternatives --set python3 /usr/bin/python3.14; \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

COPY requirements-pipeline.txt .
COPY ./spark/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"
RUN mkdir -p /opt/spark/spark-events && chmod a+wr /opt/spark/spark-events

#RUN pip3 install -r requirements-pipeline.txt

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}