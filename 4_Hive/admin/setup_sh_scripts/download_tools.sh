#!/usr/bin/env bash

#cd ..

#docker cp 4_Hive  docker-hive_hive-server_1:/homeworks/ && \
 #winpty docker-compose exec hive-server bash

#mkdir homeworks
#hdfs dfs -put /homeworks/4_Hive/fund/ /user/hive/warehouse/

# comands for docker
apt-get update --yes --quiet
apt-get upgrade  --yes --quiet
apt-get install libsasl2-dev --yes --quiet
apt-get install -y libsasl2-modules libsasl2-dev --yes --quiet
apt-get install python3  --yes --quiet
apt-get install python3-pip  --yes --quiet
pip3 install pyhive
apt-get install gcc --yes --quiet
pip3 install thrift
pip3 install sasl
pip3 install thrift_sasl