FROM ubuntu:18.04

RUN apt-get update --yes --quiet && \
	apt-get upgrade --yes --quiet && \
	apt-get install python3.7 --yes --quiet && \
	apt-get install python3-pip --yes --quiet && \
	apt-get install git --yes --quiet && \
	apt-get install wget --yes --quiet && \
	apt-get install nano --yes --quiet && \
	ln -s /usr/bin/python3 /usr/local/bin/python && \
	ln -s /usr/bin/pip3 usr/local/bin/pip 
	
RUN apt-get install openjdk-8-jdk --yes --quiet && \
	apt-get install openjdk-8-jre
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 

RUN apt-get install scala --yes --quiet

ENV PYSPARK_PYTHON=python3
ENV PATH=/usr/local/sbin:/usr/local/bin:/bin:/sbin:/usr/sbin:/usr/bin:
	
WORKDIR /homeworks
COPY . .
COPY 6_Java/target/6_Java-1.0-SNAPSHOT-jar-with-dependencies.jar 7_CI_CD/java/

RUN apt-get install mysql-client --yes --quiet

##ENTRYPOINT /scripts/create_db_user.sh