FROM python:3.9.5-slim

RUN mkdir -p /usr/share/man/man1
WORKDIR app/
RUN apt-get update && \
      apt-get -y install libpq-dev python3-dev gcc openjdk-11-jdk

RUN mkdir input_data
RUN mkdir output_data

COPY src src
RUN export PYTHONPATH="/app/src"
RUN pip install pyspark==3.2.1
RUN pip install pandas
RUN pip install ipython
RUN pip install openpyxl