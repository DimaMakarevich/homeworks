USE forex_db;

DROP PROCEDURE IF EXISTS usp_get_month_growth_up_close_close;

DELIMITER $$

CREATE PROCEDURE usp_get_month_growth_up_close_close()
BEGIN 
	WITH cte_last_days_in_months  AS (
		SELECT forex_table.`time`, forex_table.`open`, forex_table.`close` FROM forex_table
		WHERE `time` IN (
			SELECT MAX(`time`) FROM forex_table
			GROUP BY YEAR(`time`), MONTH(`time`))
    ), cte_first_days_in_months AS (
		SELECT forex_table.`time` AS first_day_in_month, forex_table.`open` AS first_day_in_month_open, forex_table.`close` AS first_day_in_month_close FROM forex_table
		WHERE `time` IN (
			SELECT MIN(`time`) FROM forex_table
			GROUP BY YEAR(`time`), MONTH(`time`))
	),  cte_joined_first_last_day AS (
		SELECT * FROM cte_first_days_in_months 
        JOIN cte_last_days_in_months
        ON DATE_FORMAT(cte_first_days_in_months.`first_day_in_month`, "%Y %M") = DATE_FORMAT(cte_last_days_in_months.`time`, "%Y %M") 
    ), cte_with_previous_close AS (
		SELECT cte_joined_first_last_day.`time`, cte_joined_first_last_day.`close`, COALESCE(LAG(cte_joined_first_last_day.`close`) OVER(ORDER BY cte_joined_first_last_day.`time`), cte_joined_first_last_day.`first_day_in_month_open`) AS previous_close FROM cte_joined_first_last_day
	), cte_month_growth_rate AS (
		SELECT YEAR(cte_with_previous_close.`time`) AS `year`, MONTH(cte_with_previous_close.`time`) AS `month`, (cte_with_previous_close.`close`/cte_with_previous_close.`previous_close` - 1) * 100 AS growth_rate  FROM cte_with_previous_close
    ), cte_total_year AS (
		SELECT cte_month_growth_rate.`year`, SUM(growth_rate) AS `total` FROM cte_month_growth_rate
        GROUP BY cte_month_growth_rate.`year`
    ), cte_total_year_and_month_growth_up AS (
		SELECT cte_month_growth_rate.`year`,cte_month_growth_rate.`month`, cte_month_growth_rate.`growth_rate`, cte_total_year.`total`  FROM cte_month_growth_rate
		JOIN cte_total_year 
        ON cte_month_growth_rate.`year` = cte_total_year.`year`
	),  cte_pivot_table_month_and_year AS (
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
    #SELECT * FROM cte_pivot_table_month_and_year;
   # /*
    SELECT * FROM cte_pivot_table_month_and_year 
    UNION 
    SELECT 'AVG', AVG(`January`), AVG(`February`), AVG(`March`),AVG(`April`), AVG(`May`),  AVG(`June`),
			AVG(`July`), AVG(`August`), AVG(`September`), AVG(`October`), AVG(`November`), AVG(`December`), AVG(cte_pivot_table_month_and_year.`total`)
    FROM cte_pivot_table_month_and_year; 
# */
END $$ 


DELIMITER ;
CALL usp_get_month_growth_up_close_close();


