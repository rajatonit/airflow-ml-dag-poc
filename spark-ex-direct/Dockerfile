FROM gcr.io/spark-operator/spark-py:v3.1.1

# ADD http://central.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar $SPARK_HOME/jars
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop3.jar $SPARK_HOME/jars
USER root

RUN mkdir -p /opt/hadoop/conf
RUN mkdir -p /opt/spark/conf
COPY conf/core-site.xml /opt/hadoop/conf
COPY conf/spark-env.sh $SPARK_HOME/conf
RUN pip install numpy
RUN pip install pandas

USER root

# ENTRYPOINT ["/usr/bin/entrypoint.sh"]
