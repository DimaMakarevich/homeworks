CREATE OR REPLACE PROCEDURE usp_etl_stg_to_stg2()
BEGIN
    SET query='
    WITH cte_transform_input_data AS (
        SELECT stg.`time`, DATE_FORMAT(stg.`time`, "Y") AS `year`, DATE_FORMAT(stg.`time`, "M") AS `month`, 
            CAST(REPLACE(stg.`open`, ",", ".") AS DECIMAL(10, 4)) AS `open`, CAST(REPLACE(stg.`close`, ",", ".") AS  DECIMAL(10, 4)) AS `close`, stg.`modified_timestamp`
        FROM forex_db.forex_stg stg
    ), cte_month_last_and_first_value AS (
        SELECT cte_t.`time`, cte_t.`year`, cte_t.`month`,
        FIRST_VALUE(cte_t.`open`) OVER (PARTITION BY cte_t.`year`, cte_t.`month` ORDER BY cte_t.`time`) AS `open`,
        FIRST_VALUE(cte_t.`close`) OVER (PARTITION BY cte_t.`year`, cte_t.`month` ORDER BY cte_t.`time` DESC) AS `close`,
        ROW_NUMBER() OVER (PARTITION BY cte_t.`year`, cte_t.`month` ORDER BY cte_t.`time` DESC) AS `row_number`,
        cte_t.`modified_timestamp`
        FROM cte_transform_input_data  cte_t
    ), cte_max_time_stg  AS ( 
        SELECT IF(MAX(stg2.modified_timestamp) is NULL, "2000-01-01 00:00:01.0", MAX(stg2.modified_timestamp)) AS `max_time` FROM  forex_db.forex_stg2 stg2
    ), cte_joined_with_max_time AS (
        SELECT * FROM cte_month_last_and_first_value
        JOIN cte_max_time_stg
        ON 1=1
    )
    FROM cte_joined_with_max_time cte_m
    INSERT INTO forex_db.forex_stg2 ( `time`, `year`, `month`, `open`, `close`, `prev_close`, `modified_timestamp`)
    SELECT cte_m.`time`, cte_m.`year`, cte_m.`month`, cte_m.`open`, cte_m.`close`,
          COALESCE(LAG(cte_m.`close`) OVER (ORDER BY cte_m.`time`), cte_m.`open`), cte_m.`modified_timestamp`
    WHERE cte_m.`row_number`=1 AND CAST(cte_m.`modified_timestamp` AS TIMESTAMP) > CAST(cte_m.`max_time` AS TIMESTAMP)
    ';
    EXECUTE query;
END;

