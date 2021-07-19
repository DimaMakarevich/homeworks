#!/usr/bin/env bash

load_data_into_tables() {
  path_to_csv=C:/Users/DmitryMakarevich/Desktop/homeworks/3_SQL/fund

  mysql --local-infile=1 --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db --execute="SET GLOBAL local_infile=1"
  for file in "${path_to_csv}"/*; do
    file_name=${file##*/}
    if [[ "${file_name}" =~ XAUUSD_10.*Ask ]]; then
      echo "${file}"
      mysql --local-infile=1 --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db --execute="
      LOAD DATA  LOCAL INFILE '$file'
	      INTO TABLE forex_table
        FIELDS TERMINATED BY ';'
        ENCLOSED BY '\"'
	      LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (\`time\`, @\`open\`, @\`high\`, @\`low\`, @\`close\`, @\`volume\`)
        SET \`open\`= REPLACE(@\`open\`, ',', '.'),
		      \`high\`= REPLACE(@\`high\`, ',', '.'),
          \`low\`= REPLACE(@\`low\`, ',', '.'),
          \`close\`= REPLACE(@\`close\`, ',', '.'),
          \`volume\`= REPLACE(@\`volume\`, ',', '.');"
    fi

  done
}


run_scripts() {
   path_to_sql_scripts=C:/Users/DmitryMakarevich/Desktop/homeworks/3_SQL/admin/sql_scripts

   mysql --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db < "${path_to_sql_scripts}/create_database.sql"
   mysql --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db <  "${path_to_sql_scripts}/create_forex_table.sql"
}


run_scripts
load_data_into_tables


