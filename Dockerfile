
FROM ubuntu:xenial

MAINTAINER nvidia

##### update ubuntu
RUN apt-get update \
    && apt-get install -y automake pkg-config libpcre3-dev zlib1g-dev liblzma-dev \
    && apt-get install -y curl net-tools build-essential git wget unzip vim python python-setuptools python-pip python-dev python-numpy \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

###################################
#### Install Java 8
###################################
#### ---------------------------------------------------------------
#### ---- Change below when upgrading version ----
#### ---------------------------------------------------------------
## https://download.oracle.com/otn-pub/java/jdk/8u201-b09/42970487e3af4f5aa5bca3f542482c60/jdk-8u201-linux-x64.tar.gz
ARG JAVA_MAJOR_VERSION=${JAVA_MAJOR_VERSION:-8}
ARG JAVA_UPDATE_VERSION=${JAVA_UPDATE_VERSION:-201}
ARG JAVA_BUILD_NUMBER=${JAVA_BUILD_NUMBER:-09}
ARG JAVA_DOWNLOAD_TOKEN=${JAVA_DOWNLOAD_TOKEN:-42970487e3af4f5aa5bca3f542482c60}

#### ---------------------------------------------------------------
#### ---- Don't change below unless you know what you are doing ----
#### ---------------------------------------------------------------
ARG UPDATE_VERSION=${JAVA_MAJOR_VERSION}u${JAVA_UPDATE_VERSION}
ARG BUILD_VERSION=b${JAVA_BUILD_NUMBER}

ENV JAVA_HOME /usr/jdk1.${JAVA_MAJOR_VERSION}.0_${JAVA_UPDATE_VERSION}
ENV PATH $PATH:$JAVA_HOME/bin
ENV INSTALL_DIR /usr
RUN curl -sL --retry 3 --insecure \
  --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
  "http://download.oracle.com/otn-pub/java/jdk/${UPDATE_VERSION}-${BUILD_VERSION}/${JAVA_DOWNLOAD_TOKEN}/jdk-${UPDATE_VERSION}-linux-x64.tar.gz" \
  | gunzip \
  | tar x -C $INSTALL_DIR/ \
  && ln -s $JAVA_HOME $INSTALL_DIR/java \
  && rm -rf $JAVA_HOME/man

############################
#### --- JAVA_HOME --- #####
############################
ENV JAVA_HOME=$INSTALL_DIR/java

###################################
#### define working directory. ####
###################################
RUN mkdir -p /data 

VOLUME "/data"

WORKDIR /data

#### Define default command.
CMD ["/bin/bash"]