FROM apache/airflow:2.3.4

USER root

# Install open-jdk
RUN apt-get install -y openjdk-8-jdk

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
RUN export JAVA_HOME 

# install pyspark and providers
ENV SPARK_HOME /home/airflow/.local/lib/python3.7/site-packages/pyspark
RUN export SPARK_HOME

USER airflow 
RUN pip install pyspark && \
    pip install apache-airflow-providers-apache-spark && \
    pip install apache-airflow-providers-telegram


