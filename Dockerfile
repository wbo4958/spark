FROM ubuntu:xenial

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    openjdk-8-jdk \
    wget

# Build script looks for javac in jre dir
ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64"

# http://spark.apache.org/docs/latest/building-spark.html#setting-up-mavens-memory-usage
# We have a pretty beefy server
ENV MAVEN_OPTS "-Xmx20g -XX:ReservedCodeCacheSize=2g"
