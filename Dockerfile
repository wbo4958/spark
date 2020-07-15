##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##
FROM ubuntu:xenial-20200212

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
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    llvm \
    zip

ENV PYTHON_ROOT /usr
ENV PYTHON_VERSION 3.6.10

RUN wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xvf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --prefix="${PYTHON_ROOT}" && \
    make && \
    make install && \
    cd ..

RUN rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    rm -r /root/.cache && rm -rf /var/cache/apt/*

RUN DEBIAN_FRONTEND=noninteractive pip3 install \
    flake8 \
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
