FROM centos:8

RUN yum update -y  && \
	yum upgrade -y  && \
	yum install python3 -y && \
	yum install python3-pip -y && \
	yum install git -y && \
	yum install wget -y && \
	yum install nano -y && \
	ln -s /usr/bin/python3 /usr/local/bin/python && \
	ln -s /usr/bin/pip3 usr/local/bin/pip 
	
RUN yum install java-1.8.0-openjdk -y && \
	yum install  java-1.8.0-openjdk-devel -y
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.292.b10-0.el8_3.x86_64

RUN wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.rpm && \ 
	yum install  scala-2.11.8.rpm -y  
	

RUN wget  http://ftp.byfly.by/pub/apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz -q -O spark.tgz && \
	tar -xzf spark.tgz && \
	ln -s spark-3.0.2-bin-hadoop2.7 spark  && \
	#pip install findspark && \
	#pip install pyspark~=3.0.2  && \
	rm -rf spark.tgz
	
	
	
ENV SPARK_HOME=spark
ENV PYSPARK_PYTHON=python3
ENV PATH=/usr/local/sbin:/usr/local/bin:/bin:/sbin:/usr/sbin:/usr/bin:${SPARK_HOME}/bin	
	
WORKDIR /homeworks
COPY . .

RUN yum install mysql -y 

RUN echo $'[mongodb-org-4.4]\nname=MongoDB Repository \nbaseurl=https://repo.mongodb.org/yum/redhat/$releasever/mongodb-org/4.4/x86_64/ \ngpgcheck=1 \nenabled=1 \ngpgkey = https://www.mongodb.org/static/pgp/server-4.4.asc'  \ 
	> /etc/yum.repos.d/mongodb-org-4.4.repo && \ 																															
	yum install -y mongodb-org && \
	yum clean all