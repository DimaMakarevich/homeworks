USE forex_db;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/XAUUSD_10 Secs_Ask_2020.01.01_2020.07.17.csv'
	INTO TABLE forex_table
    FIELDS TERMINATED BY ';'
    ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
    IGNORE 1 LINES
    (`time`, @`open`, @`high`, @`low`, @`close`, @`volume`)
    SET `open`= REPLACE(@`open`, ',', '.'),
		`high`= REPLACE(@`high`, ',', '.'),
        `low`= REPLACE(@`low`, ',', '.'),
        `close`= REPLACE(@`close`, ',', '.'),
        `volume`= REPLACE(@`volume`, ',', '.');
    