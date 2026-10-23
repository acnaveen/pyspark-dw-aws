# Python 3.12 base image for PySpark 4.2
FROM python:3.12-bookworm

EXPOSE 8888 4040
SHELL ["/bin/bash", "-c"]

RUN pip install --upgrade pip

# Install OpenJDK 17 and utilities
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget git nano vim ca-certificates-java && \
    update-ca-certificates -f && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Dynamically resolve JAVA_HOME regardless of host architecture (amd64/arm64)
ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN ln -s $(readlink -f /usr/bin/java | sed "s:bin/java::") $JAVA_HOME

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Avoid symlinking site-packages directly to a root folder name to prevent conf masking
ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark
ENV SPARK_CONF_DIR=/etc/spark/conf
ENV PATH=$PATH:$SPARK_HOME/bin

# Postgres JDBC driver for the Hive metastore
RUN wget -q -P $SPARK_HOME/jars \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

RUN mkdir -p $SPARK_CONF_DIR /root/.aws

WORKDIR /pyspark-dw-aws

# Spark/Hive configs come from the build context (config_files/ next to this Dockerfile).
# AWS credentials are NOT baked in; docker-compose mounts ./.aws at runtime.
COPY config_files/spark-defaults.conf $SPARK_CONF_DIR/spark-defaults.conf
COPY config_files/hive-site.xml       $SPARK_CONF_DIR/hive-site.xml

RUN ipython profile create && \
    echo "c.IPKernelApp.capture_fd_output = False" >> "/root/.ipython/profile_default/ipython_kernel_config.py"

CMD ["python3", "-m", "jupyterlab", "--ip", "0.0.0.0", "--allow-root", "--no-browser"]