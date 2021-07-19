USE forex_db;
DROP TABLE IF EXISTS forex_audit;
CREATE TABLE forex_db.forex_audit (
    `checksum` VARCHAR(64),
    `start_timestamp` TIMESTAMP,
    `file_name` VARCHAR(100),
    `task_name` VARCHAR(25),
    `lines_insert` BIGINT,
    `end_time` TIMESTAMP,
    PRIMARY KEY (`checksum`) DISABLE NOVALIDATE
)
clustered by (`checksum`) into 1 buckets 
STORED AS orc tblproperties ("transactional"="true" );




 




