FROM openjdk:11-jre-slim

RUN apt-get update && apt-get install -y wget tar

RUN apt-get update && apt-get install -y \
    wget \
    tar \
    curl \
    bash \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.5
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz -C /opt \
    && ln -s /opt/spark-$SPARK_VERSION-bin-hadoop3 /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz

ENV SBT_VERSION=1.10.11
RUN curl -L -o sbt-$SBT_VERSION.tgz https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz \
    && tar -xzf sbt-$SBT_VERSION.tgz \
    && rm sbt-$SBT_VERSION.tgz \
    && mv sbt /usr/local

ENV JAVA_OPTS="-Xms512m -Xmx2g"
ENV SBT_OPTS="-Xms512m -Xmx2g -XX:+UseG1GC"

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:/usr/local/sbt/bin:$PATH

WORKDIR /app

COPY build.sbt /app/
COPY project /app/project/

RUN sbt update

COPY . /app/

RUN sbt package

CMD ["spark-submit", "--class", "runner.MainRunner", "target/scala-2.12/lsh-test_2.12-0.1.0-SNAPSHOT.jar"]