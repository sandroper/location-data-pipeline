FROM apache/spark:4.0.1-scala2.13-java17-ubuntu

USER root

# Install Python 3.14 with pip support and build tools for compiling packages
RUN set -ex; \
    apt-get upgrade; \
    apt-get update; \
    apt install -y software-properties-common curl build-essential; \
    add-apt-repository ppa:deadsnakes/ppa; \
    apt-get update; \
    apt-get install -y python3.14 python3.14-venv python3.14-dev; \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 10; \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.14 14; \
    update-alternatives --set python3 /usr/bin/python3.14; \
    rm -rf /var/lib/apt/lists/*

# Install pip for Python 3.14 using get-pip.py
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.14

ENV SPARK_HOME=/opt/spark

COPY requirements-pipeline.txt .
COPY ./spark/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"
RUN mkdir -p /opt/spark/spark-events && chmod a+wr /opt/spark/spark-events

# Install Python dependencies for PySpark workers
RUN python3.14 -m pip install -r requirements-pipeline.txt

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}