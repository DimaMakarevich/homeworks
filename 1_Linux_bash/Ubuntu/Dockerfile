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

RUN wget  http://ftp.byfly.by/pub/apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz -q -O spark.tgz && \
	tar -xzf spark.tgz && \
	ln -s spark-3.0.2-bin-hadoop2.7 spark  && \
	#pip install findspark && \
	#pip install pyspark~=3.0.2  && \
	rm -rf spark.tgz

ENV SPARK_HOME = spark
ENV PYSPARK_PYTHON=python3
ENV PATH=/usr/local/sbin:/usr/local/bin:/bin:/sbin:/usr/sbin:/usr/bin:${SPARK_HOME}/bin	
	
WORKDIR /homeworks
COPY . .

RUN apt-get install mysql-client --yes --quiet

RUN apt-get install mongodb --yes --quiet && \
	rm -rf /var/lib/apt/lists/*   #очистка кэша



