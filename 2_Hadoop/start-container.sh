#!/usr/bin/env bash

docker network inspect hadoop >/dev/null 2>&1 || \
  docker network create --driver bridge hadoop

N=2

docker rm -f hdfs-namenode
echo "Start NameNode"
docker run -itd --net=hadoop \
                -p 50070:50070 \
                -p 8088:8088 \
                --name hdfs-namenode \
                --hostname hdfs-namenode \
                dmitrymakarevich/hadoop:1.0

i=1
while [ $i -le $N ]
do
    docker rm -f hdfs-datanode${i}
    echo "Start ${i} DataNode"
    docker run -itd --net=hadoop \
                    --name hdfs-datanode${i} \
                    --hostname hdfs-datanode${i} \
                    dmitrymakarevich/hadoop:1.0
    i=$(( $i +1 ))
done

cd ..

# dot to copy all homeworks repository
docker cp 4_Hive  hdfs-namenode:/homeworks && \
winpty docker exec -ti hdfs-namenode  bash usr/local/hadoop/sbin/start-all.sh && \
#winpty docker exec -ti hdfs-namenode bash hdfs dfs -mkdir -p /tmp /user/hive/warehouse  && \
#winpty docker exec -ti hdfs-namenode bash usr/local/hive/bin/schematool -dbType derby -initSchema && \
#winpty docker exec -ti  hdfs-namenode bash hdfs dfs -put /homeworks/fund/ /user/hive/warehouse && \
winpty docker exec -ti hdfs-namenode bash