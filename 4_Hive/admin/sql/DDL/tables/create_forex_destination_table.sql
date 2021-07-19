USE forex_db;
DROP TABLE IF EXISTS forex_destination;
CREATE TABLE forex_db.forex_destination (
    `year` VARCHAR(5),
    `jan` DECIMAL(10, 4),
    `feb` DECIMAL(10, 4),
    `mar` DECIMAL(10, 4),
    `apr` DECIMAL(10, 4),
    `may` DECIMAL(10, 4),
    `jun` DECIMAL(10, 4),
    `jul` DECIMAL(10, 4),
    `aug` DECIMAL(10, 4),
    `sep` DECIMAL(10, 4),
    `oct` DECIMAL(10, 4),
    `nov` DECIMAL(10, 4),
    `dec` DECIMAL(10, 4),
    `total` DECIMAL(10, 4),
    PRIMARY KEY (`year`) DISABLE NOVALIDATE
)
LOCATION '/user/hive/warehouse/forex_db.db/forex_destination';




 




