#!/usr/bin/env bash

DATA_PATH=/homeworks/4_Hive/fund
PYTHON_SCRIPT_PATH=/homeworks/4_Hive/admin/python
TASK_NAME="FOREX_MONTH_GROWTH_RATE"

incremental_load() {
  for DATA_FILE in "${DATA_PATH}"/*.csv; do
    sha256=$(python3 ${PYTHON_SCRIPT_PATH}/checksum.py ${DATA_FILE})
    is_hash_exist=$(bash /opt/hive/bin/hive -e "SELECT COUNT(*) FROM forex_db.forex_audit WHERE checksum = '${sha256}' LIMIT 1;")
    if [[ ${is_hash_exist} == "0" ]] ; then
      start_time=$(date +"%Y-%m-%d %H:%M:%S")
      echo "INSERT INTO AUDIT TABLE"
      bash /opt/hive/bin/hive -e "INSERT INTO forex_db.forex_audit VALUES ('${sha256}', '${start_time}', '${DATA_FILE}', '${TASK_NAME}', NULL, NULL)"
      echo "LOAD DATA WITH PYTHON SCRIPT"
      python3 ${PYTHON_SCRIPT_PATH}/load_data.py ${DATA_FILE}
      echo "UPDATE STG TABLE ADD MODIFIED TIMESTAMP"
      modified_timestamp=$(date +"%Y-%m-%d %H:%M:%S")
      bash /opt/hive/bin/hive -e "FROM forex_db.forex_stg stg INSERT OVERWRITE TABLE forex_db.forex_stg SELECT stg.time, stg.open, stg.high, stg.low, stg.close, stg.volume, IF(stg.modified_timestamp is NULL, '${modified_timestamp}',stg.modified_timestamp)"
      echo ${DATA_FILE}
      end_time=$(date +"%Y-%m-%d %H:%M:%S")
      lines_in_file=$(wc -l  < ${DATA_FILE})
      echo "UPDATE AUDIT TABLE"
      bash /opt/hive/bin/hplsql -e "UPDATE forex_db.forex_audit SET lines_insert = '${lines_in_file}', end_time = '${end_time}'  WHERE checksum = '${sha256}'"
    fi
  done
}

incremental_load