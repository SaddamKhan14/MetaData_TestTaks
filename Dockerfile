FROM python:3.9.7-alpine3.14

ARG SPARK_VERSION=3.0.1
ARG HADOOP_VERSION_SHORT=3.2
ARG HADOOP_VERSION=3.2.0
ARG AWS_SDK_VERSION=1.11.375

RUN apk add --no-cache bash openjdk8-jre && \
  apk add --no-cache libc6-compat && \
  ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2 && \
  pip install findspark

# Download and extract Spark
RUN wget -qO- https://www-eu.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT}.tgz | tar zx -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT} /opt/spark

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"
# Define default command

RUN mkdir $SPARK_HOME/conf
RUN echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh

#Copy python script
copy MetaData_TestTaks/* /app/MetaData_TestTaks

# Define default command
CMD ["/bin/bash"]