FROM python:3.9.5-slim

RUN mkdir -p /usr/share/man/man1

RUN apt-get update && \
      apt-get -y install libpq-dev python3-dev gcc \
      wget procps tar less vim sudo gnupg gnupg2 gnupg1 software-properties-common iputils-ping

RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | sudo apt-key add -

RUN sudo add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/

RUN sudo apt-get update && sudo apt-get -y install adoptopenjdk-8-hotspot

RUN wget --quiet https://downloads.apache.org/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz
RUN wget --quiet https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz

RUN tar -xf apache-hive-3.1.2-bin.tar.gz -C /opt
RUN tar -xf hadoop-3.3.1.tar.gz -C /opt
RUN mv /opt/apache-hive-3.1.2-bin /opt/hive
RUN mv /opt/hadoop-3.3.1 /opt/hadoop

RUN mkdir -p /opt/hive/hcatalog/var/log
RUN mkdir -p /tmp/hive
RUN chmod 777 /tmp/hive

EXPOSE 9083
EXPOSE 10000
EXPOSE 10001

WORKDIR app/

COPY src src
RUN pip install pyspark==3.2.1
RUN pip install pandas
RUN pip install ipython
RUN pip install openpyxl
