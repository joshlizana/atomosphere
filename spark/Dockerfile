FROM apache/spark:4.0.2-python3

USER root

# Iceberg + Polaris REST catalog + S3/RustFS connectivity
ENV ICEBERG_VERSION=1.10.1
ENV SPARK_ICEBERG_COMPAT=4.0
ENV SCALA_VERSION=2.13

RUN curl -fSL -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_ICEBERG_COMPAT}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_ICEBERG_COMPAT}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_ICEBERG_COMPAT}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar" && \
    curl -fSL -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

RUN pip install --no-cache-dir websocket-client

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY sources/ /opt/spark/work-dir/spark/sources/
COPY ingestion/ /opt/spark/work-dir/spark/ingestion/

USER spark

ENV PYTHONPATH="/opt/spark/work-dir"
