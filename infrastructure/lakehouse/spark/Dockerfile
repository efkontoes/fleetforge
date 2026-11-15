# Spark jobs and lakehouse tests. Build context: repository root.
#
# Python 3.12 (the project's version, same base as the application image) with a
# Temurin 21 JRE copied in, rather than the apache/spark image: Spark local mode
# runs the Python row stage in worker processes of THIS interpreter, so the
# project's dependency lock must be installed here anyway.
FROM eclipse-temurin:21.0.12.1_1-jre-noble@sha256:cd35bb33859641c5374415826a5951845104093eaa401cbe9d3d9023444f1c13 AS jre

FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements-lakehouse.lock ./
RUN pip install -r requirements-lakehouse.lock

# Resolve the pinned Spark packages once, at build time (Ivy downloads them with
# their transitive dependencies from Maven Central). Jobs then load the baked jars
# (FLEETFORGE_LAKEHOUSE_JARS_DIR) and never download at run time.
ARG SPARK_PACKAGES=org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0,org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4,org.postgresql:postgresql:42.7.14
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').config('spark.jars.packages', '${SPARK_PACKAGES}').config('spark.jars.ivy', '/opt/ivy').getOrCreate().stop()" \
 && mkdir -p /opt/spark-jars && cp /opt/ivy/jars/*.jar /opt/spark-jars/ && rm -rf /opt/ivy
ENV FLEETFORGE_LAKEHOUSE_JARS_DIR=/opt/spark-jars

COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-deps .
RUN useradd --uid 10001 --create-home fleetforge
USER fleetforge
