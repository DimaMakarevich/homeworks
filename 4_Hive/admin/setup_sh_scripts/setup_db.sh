#!/usr/bin/env bash

PYTHON_SCRIPT_PATH=/homeworks/4_Hive/admin/python
SH_SCRIPT_PATH=/homeworks/4_Hive/admin/

SQL_SCRIPT_PATH=/homeworks/4_Hive/admin/sql

create_databases() {
  for SCRIPT_PATH in "${SQL_SCRIPT_PATH}"/DDL/databases/*.sql; do
    echo $SCRIPT_PATH
     bash /opt/hive/bin/hive -f ${SCRIPT_PATH}
    done
}


create_tables() {
  for SCRIPT_PATH in "${SQL_SCRIPT_PATH}"/DDL/tables/*.sql; do
    echo $SCRIPT_PATH
     bash /opt/hive/bin/hive -f ${SCRIPT_PATH}
    done
}


create_usp() {
  for SCRIPT_PATH in "${SQL_SCRIPT_PATH}"/usp/*.sql; do
    echo $SCRIPT_PATH
     bash /opt/hive/bin/hplsql -f ${SCRIPT_PATH}
    done

}
create_databases

create_tables

create_usp

python3 "${SH_SCRIPT_PATH}"/incremental_load.sh

hplsql -e " INCLUDE '/homeworks/4_Hive/admin/sql/usp/usp_etl_stg_to_stg2.sql' CALL usp_etl_stg_to_stg2();"

#MAX_TIMESTAMP_STG2=$(bash /opt/hive/bin/hive -e " SELECT IF(MAX(stg2.modified_timestamp) is NULL, '2000-00-00 00:00:00.0', MAX(stg2.modified_timestamp)) FROM  forex_db.forex_stg2 stg2;")


#hplsql -f "/homeworks/4_Hive/admin/sql/usp/usp_etl_stg_to_stg2.sql"

#hplsql -e " INCLUDE '/homeworks/4_Hive/admin/sql/usp/usp_etl_stg2_to_destination.sql' CALL usp_etl_stg2_to_destination();"


#CALL usp_etl_stg2_to_distination("oc");
#python3 load data

#apt-get update --yes --quiet && \
#apt-get upgrade --yes --quiet && \
#apt-get install python3.7 --yes --quiet && \
#apt-get install python3-pip && \
#apt-get install libsasl2-dev
#2012-01-20 00:05:50.0



#apt-get update && \ apt-get upgrade
#apt-get install libsasl2-dev
#apt-get install -y libsasl2-modules libsasl2-dev
#apt-get install python3
#apt-get install python3-pip
#pip3 install pyhive
#apt-get install gcc
#pip3 install thrift
#pip3 install sasl
#pip3 install thrift_sasl


#pip install pyhs2
#apt-get install python-pip
#apt-get install python2.7

#cd /usr/lib/python2.7
#sudo ln -s plat-x86_64-linux-gnu/_sysconfigdata_nd.py .


#LOAD DATA INPATH '/user/hive/warehouse/fund/XAUUSD_10Secs_Ask_2020.01.01_2020.07.17.csv'  INTO TABLE  forex_db.forex_stg;