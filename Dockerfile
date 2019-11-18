FROM ubuntu:xenial

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    curl \
    flake8 \
    git-core \
    maven \
    openjdk-8-jdk \
    python3 \
    python3-pip \
    python-numpy \
    scala \
    software-properties-common \
    python-software-properties \
    wget

RUN apt-get install -y \
    locales \
    language-pack-fi \
    language-pack-en && \
    export LANGUAGE=en_US.UTF-8 && \
    export LANG=en_US.UTF-8 && \
    export LC_ALL=en_US.UTF-8 && \
    locale-gen en_US.UTF-8 && \
    dpkg-reconfigure locales

RUN add-apt-repository -y ppa:jonathonf/python-3.6 && \
  apt-get update && \
  apt-get install -y python3.6

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.6 1 && \
  update-alternatives --install /usr/bin/python python /usr/bin/python3.6 2 && \
  update-alternatives --set python3 /usr/bin/python3.6 && \
  update-alternatives --set python /usr/bin/python3.6

RUN DEBIAN_FRONTEND=noninteractive pip3 install \
    requests \
    numpy

# Build script looks for javac in jre dir
ENV JAVA_HOME "/usr/lib/jvm/java-8-openjdk-amd64"
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# http://spark.apache.org/docs/latest/building-spark.html#setting-up-mavens-memory-usage
# We have a pretty beefy server
ENV MAVEN_OPTS "-Xmx20g -XX:ReservedCodeCacheSize=2g"

RUN adduser --uid 26576 --gid 30 --shell /bin/bash svcngcc
