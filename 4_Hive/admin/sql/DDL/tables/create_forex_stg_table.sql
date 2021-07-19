USE forex_db;
DROP TABLE IF EXISTS forex_stg;
CREATE TABLE forex_db.forex_stg (
    `time` TIMESTAMP,
    `open` VARCHAR(10), 
    high VARCHAR(10),
    low VARCHAR(10),
    `close` VARCHAR(10),
    volume VARCHAR(15),
    `modified_timestamp` TIMESTAMP,
    PRIMARY KEY (`time`) DISABLE NOVALIDATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/forex_db.db/forex_stg'
TBLPROPERTIES('skip.header.line.count'='1');
