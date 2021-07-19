USE forex_db;
DROP TABLE IF EXISTS forex_stg2;
CREATE TABLE forex_db.forex_stg2 (
    `time` TIMESTAMP,
    `year` SMALLINT,
    `month` SMALLINT,
    `open` DECIMAL(10, 4),
    `close` DECIMAL(10, 4),
    `prev_close` DECIMAL(10, 4),
    `modified_timestamp` TIMESTAMP,
    PRIMARY KEY (`time`) DISABLE NOVALIDATE
)
LOCATION '/user/hive/warehouse/forex_db.db/forex_stg2';




 




