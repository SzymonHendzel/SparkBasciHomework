#Use Ubuntu as the base image for better compatibility
FROM ubuntu:22.04

# Define build arguments for Hadoop version and Spark user ID
ARG HADOOP_VERSION=3.3.5
ARG SPARK_UID=185

# Set environment variables for Spark and Hadoop configurations
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV PATH $PATH:$SPARK_HOME/bin
ENV PYTHONUNBUFFERED=1
ENV SPARK_VERSION=3.5.4
ENV SPARK_USER sparkuser  # Default user for Spark
# Java options for Spark driver and executor to define user home and Ivy home directories
ENV SPARK_DRIVER_EXTRA_JAVA_OPTIONS="-Duser.home=/tmp -Divy.home=/tmp/.ivy2"
ENV SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="-Duser.home=/tmp -Divy.home=/tmp/.ivy2"
ENV IVY_HOME="/tmp/.ivy2"  

# Install system dependencies and Python 3 without interactive prompts
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    bash curl wget openjdk-11-jdk libstdc++6 glibc-source krb5-user libnss3 tini python3 python3-pip python3-setuptools \
    && rm -rf /var/lib/apt/lists/*  # Clean up to reduce image size

# Ensure Python3 is the default Python interpreter
RUN ln -s /usr/bin/python3 /usr/bin/python

# Download and install Apache Spark from the official archive
RUN wget -O /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" && \
    tar -xzf /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-without-hadoop ${SPARK_HOME} && \
    rm -f /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz  # Clean up downloaded file

# Download and install Hadoop from the official archive
RUN wget -O /tmp/hadoop-${HADOOP_VERSION}.tar.gz \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm -f /tmp/hadoop-${HADOOP_VERSION}.tar.gz  # Clean up downloaded file

# Set Spark classpath to include Hadoop libraries for integration
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/tools/lib/*"

# Copy necessary files into the image
COPY ./entrypoint.sh /opt/  
COPY ./requirements.txt /opt/  
COPY main.py /opt/spark-3.5.4-bin-without-hadoop/work-dir/main.py  

# Install additional Python packages required for the application using pip
RUN pip3 install --upgrade pip setuptools pygeohash requests && \
	pip3 install -r /opt/requirements.txt && \
    rm -rf /var/cache/apk/*  # Clean up cache to reduce image size

# Ensure entrypoint script is executable
RUN chmod +x /opt/*.sh

# Ensure tini is installed and create a symlink for compatibility with init systems
RUN ln -s /usr/bin/tini /sbin/tini

# Create a non-root user for security purposes, minimizing risk of privilege escalation
RUN groupadd -r spark && useradd -r -g spark -u ${SPARK_UID} spark

# Set the working directory where the application will run
WORKDIR /opt/spark-3.5.4-bin-without-hadoop/work-dir

# Specify the entry point of the container, which will execute when the container starts
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Switch to the non-root user created earlier to run the application securely
USER ${SPARK_UID}