FROM ubuntu:18.04

RUN apt-get update --yes --quiet && \
	apt-get upgrade --yes --quiet && \
	apt-get install python3.7 --yes --quiet && \
	apt-get install python3-pip --yes --quiet && \
	apt-get install git --yes --quiet && \
	apt-get install wget --yes --quiet && \
	apt-get install nano --yes --quiet && \
	apt-get install rsync --yes --quiet && \
	apt-get install openssh-server --yes --quiet && \
	ln -s /usr/bin/python3 /usr/local/bin/python && \
	ln -s /usr/bin/pip3 usr/local/bin/pip 
	
RUN apt-get install openjdk-8-jdk --yes --quiet && \
	apt-get install openjdk-8-jre
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

RUN apt-get install scala --yes --quiet

RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz -q && \
    tar -xzf hadoop-2.7.0.tar.gz && \
    mv hadoop-2.7.0 /usr/local/hadoop  && \
    rm hadoop-2.7.0.tar.gz && \
    mkdir /var/log/hadoop
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_COMMON_HOME=/usr/local/hadoop
ENV HADOOP_HDFS_HOME=/usr/local/hadoop
ENV HADOOP_MAPRED_HOME=/usr/local/hadoop
ENV HADOOP_YARN_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV HADOOP_LOG_DIR=/var/log/hadoop

RUN wget http://ftp.byfly.by/pub/apache.org/spark/spark-3.0.2/spark-3.0.2-bin-hadoop2.7.tgz -q -O spark.tgz && \
	tar -xzf spark.tgz && \
	ln -s spark-3.0.2-bin-hadoop2.7 spark  && \
	rm -rf spark.tgz
ENV SPARK_HOME = spark
ENV PYSPARK_PYTHON=python3
#pip install findspark && \
	#pip install pyspark~=3.0.2  && \   https://mirror.datacenter.by/

RUN wget http://ftp.byfly.by/pub/apache.org/hive/hive-2.3.9/apache-hive-2.3.9-bin.tar.gz -q -O hive.tar.gz && \
    tar -xzf hive.tar.gz && \
    mv apache-hive-2.3.9-bin /usr/local/hive
ENV HIVE_HOME=/usr/local/hive

ENV PATH=/usr/local/sbin:/usr/local/bin:/bin:/sbin:/usr/sbin:/usr/bin:${SPARK_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${HIVE_HOME}/bin

RUN rm ${HIVE_HOME}/lib/guava-*.jar && \
    cp ${HADOOP_HOME}/share/hadoop/hdfs/lib/guava-*.jar ${HIVE_HOME}/lib/

COPY config/* /tmp/

RUN mv /tmp/hive-site.xml ${HIVE_HOME}/conf && \
    mv /tmp/hplsql-site.xml ${HIVE_HOME}/conf && \
    mv /tmp/*-site.xml ${HADOOP_HOME}/etc/hadoop/ && \
    mv /tmp/hadoop-env.sh ${HADOOP_HOME}/etc/hadoop && \
    mv /tmp/slaves $HADOOP_HOME/etc/hadoop/slaves && \
    mv /tmp/ssh_config ~/.ssh/config

RUN apt-get install mysql-client --yes --quiet

RUN apt-get install mongodb --yes --quiet && \
	rm -rf /var/lib/apt/lists/*   #очистка кэша

RUN ${HADOOP_HOME}/bin/hdfs namenode -format

CMD ["/bin/bash", "-c", "service ssh start; bash"]

