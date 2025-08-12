FROM apache/airflow:2.9.2-python3.12

COPY requirements.txt /requirements.txt

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
	apt-get install -y ant && \
	apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow