USE forex_db;

DROP TABLE IF EXISTS forex_table;
CREATE TABLE forex_table (
		`time` TIMESTAMP,
		`open` DECIMAL(9, 5) NOT NULL, 
        `high` DECIMAL(9, 5) NOT NULL,
        `low` DECIMAL(9, 5) NOT NULL,
        `close` DECIMAL(9, 5) NOT NULL,
        `volume` DECIMAL(9, 5) NOT NULL,
        CONSTRAINT pk_time_utc PRIMARY KEY (`time`)
)