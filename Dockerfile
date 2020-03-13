FROM ubuntu:bionic-20200219

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    curl \
    flake8 \
    git-core \
    maven \
    openjdk-8-jdk \
    scala \
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

RUN apt-get update && \
    apt install -y python python-pip && \
    apt install -y python3 python3-pip && \
    # We remove ensurepip since it adds no functionality since pip is
    # installed on the image and it just takes up 1.6MB on the image
    rm -r /usr/lib/python*/ensurepip && \
    pip install --upgrade pip setuptools && \
    # You may install with python3 packages by using pip3.6
    # Removed the .cache to save space
    rm -r /root/.cache && rm -rf /var/cache/apt/*

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

RUN adduser --disabled-password --gecos "" --uid 26576 --gid 30 --shell /bin/bash svcngcc
