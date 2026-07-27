FROM jupyter/pyspark-notebook:spark-3.5.0

# Versions aligned with compose.yaml
ARG SPLINE_AGENT_VERSION=2.3.0
ARG ICEBERG_VERSION=1.10.1
ARG SCALA_VERSION=2.12

USER root

# Spark 3.5.0 only supports Java 8/11; the base image ships Java 17 which
# breaks the Spline listener's URLClassLoader reflection. Install Java 11.
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl openjdk-11-jre-headless \
    && update-alternatives --set java /usr/lib/jvm/java-11-openjdk-amd64/bin/java \
    && rm -rf /var/lib/apt/lists/*

# Spline Spark agent bundle (driver-side, used as a --jars).
# Install into $SPARK_HOME/jars so it shows up on the classpath out of the box.
RUN curl -fsSL -o $SPARK_HOME/jars/spline-spark-agent-bundle.jar \
       "https://repo1.maven.org/maven2/za/co/absa/spline/agent/spark/spark-3.5-spline-agent-bundle_${SCALA_VERSION}/${SPLINE_AGENT_VERSION}/spark-3.5-spline-agent-bundle_${SCALA_VERSION}-${SPLINE_AGENT_VERSION}.jar"

# Iceberg Spark runtime — same idea.
RUN curl -fsSL -o $SPARK_HOME/jars/iceberg-spark-runtime.jar \
       "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"

# Global Spark defaults wiring everything together.
COPY conf/ $SPARK_HOME/conf/

USER ${NB_UID}
WORKDIR /home/jovyan/work
