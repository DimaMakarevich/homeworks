USE forex_db;

DROP PROCEDURE IF EXISTS usp_get_month_growth_up_open_close;

DELIMITER $$

CREATE PROCEDURE usp_get_month_growth_up_open_close()
BEGIN 
	WITH cte_last_days_in_month AS (
		SELECT * FROM forex_table
		WHERE `time` IN (
			SELECT MAX(`time`) FROM forex_table
			GROUP BY YEAR(`time`), MONTH(`time`))
	), cte_first_days_in_month AS (	
		SELECT * FROM forex_table
		WHERE `time` IN (
			SELECT MIN(`time`) FROM forex_table
			GROUP BY YEAR(`time`), MONTH(`time`))
	), cte_month_growth_rate AS (   
		SELECT YEAR(cte_last_days_in_month.`time`) AS `year`, MONTH(cte_last_days_in_month.`time`) AS `month`, (cte_last_days_in_month.`close`/cte_first_days_in_month.`open` - 1) * 100 AS growth_rate  FROM cte_last_days_in_month
        JOIN cte_first_days_in_month 
        ON MONTH(cte_last_days_in_month.`time`) = MONTH(cte_first_days_in_month.`time`) and YEAR(cte_first_days_in_month.`time`) = YEAR(cte_last_days_in_month.`time`)
	), cte_total_year AS (
		SELECT cte_month_growth_rate.`year`, SUM(growth_rate) AS `total` FROM cte_month_growth_rate
        GROUP BY cte_month_growth_rate.`year`
    ), cte_total_year_and_month_growth_up AS (
		SELECT cte_month_growth_rate.`year`, cte_month_growth_rate.`month`, cte_month_growth_rate.`growth_rate`, cte_total_year.`total`  FROM cte_month_growth_rate
		JOIN cte_total_year 
        ON cte_month_growth_rate.`year` = cte_total_year.`year`
    ), cte_pivot_table_month_and_year AS (
		SELECT cte_total_year_and_month_growth_up.`year`,
        SUM(IF(`month`=1, growth_rate, NULL)) AS `January`,
		SUM(IF(`month`=2, growth_rate, NULL)) AS `February`,
		SUM(IF(`month`=3, growth_rate, NULL)) AS `March`,
		SUM(IF(`month`=4, growth_rate, NULL)) AS `April`,
		SUM(IF(`month`=5, growth_rate, NULL)) AS `May`,
		SUM(IF(`month`=6, growth_rate, NULL)) AS `June`,
		SUM(IF(`month`=7, growth_rate, NULL)) AS `July`,
		SUM(IF(`month`=8, growth_rate, NULL)) AS `August`,
        SUM(IF(`month`=9, growth_rate, NULL)) AS `September`,
        SUM(IF(`month`=10, growth_rate, NULL)) AS `October`,
        SUM(IF(`month`=11, growth_rate, NULL)) AS `November`,
        SUM(IF(`month`=12, growth_rate, NULL)) AS `December`,
		cte_total_year_and_month_growth_up.`total`
        FROM cte_total_year_and_month_growth_up
        GROUP BY cte_total_year_and_month_growth_up.`year`
    )
    #SELECT * FROM cte_last_days_in_month
    #/*
    SELECT * FROM cte_pivot_table_month_and_year
    UNION 
    SELECT 'AVG', AVG(`January`), AVG(`February`), AVG(`March`),AVG(`April`), AVG(`May`),  AVG(`June`),
			AVG(`July`), AVG(`August`), AVG(`September`), AVG(`October`), AVG(`November`), AVG(`December`), AVG(cte_pivot_table_month_and_year.`total`)
    FROM cte_pivot_table_month_and_year
   # */
    ;
 END $$ 
 
 
DELIMITER ;
 
 CALL usp_get_month_growth_up_open_close();
 