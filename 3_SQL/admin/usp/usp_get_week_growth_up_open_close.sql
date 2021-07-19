USE forex_db;

DROP PROCEDURE IF EXISTS usp_get_week_growth_up_open_close;

DELIMITER $$

CREATE PROCEDURE usp_get_week_growth_up_open_close()
BEGIN 
	WITH cte_last_time_in_day AS (
		SELECT * FROM forex_table
		WHERE `time` IN (
			SELECT MAX(`time`) FROM forex_table
			GROUP BY YEAR(`time`), MONTH(`time`), DAY(`time`))
	), cte_first_time_in_day AS (	
		SELECT * FROM forex_table
		WHERE `time` IN (
			SELECT MIN(`time`) FROM forex_table
			GROUP BY YEAR(`time`), MONTH(`time`), DAY(`time`))
	), cte_days_growth_up AS (
		SELECT cte_last_time_in_day.*, (cte_last_time_in_day.`close`/cte_first_time_in_day.`open` - 1) * 100 AS growth_up FROM cte_last_time_in_day
        JOIN cte_first_time_in_day
        ON DATE_FORMAT(cte_last_time_in_day.`time` ,"%Y %M %D") = DATE_FORMAT(cte_first_time_in_day.`time` ,"%Y %M %D")
	), cte_separed_by_month_and_weekday AS (
		SELECT MONTH(cte_days_growth_up.`time`) AS 'month',  WEEKDAY(cte_days_growth_up.`time`) AS 'weekday', AVG(cte_days_growth_up.`growth_up`) AS avg_weekday_growth_up FROM cte_days_growth_up
        GROUP BY MONTH(cte_days_growth_up.`time`), WEEKDAY(cte_days_growth_up.`time`)
    ), cte_separed_by_month_and_weekday_with_total AS (
		SELECT *, AVG(avg_weekday_growth_up) AS total FROM cte_separed_by_month_and_weekday
        GROUP BY cte_separed_by_month_and_weekday.`month`
	), cte_joinded_total_and_week AS (
		SELECT cte_separed_by_month_and_weekday.*, cte_separed_by_month_and_weekday_with_total.`total` FROM cte_separed_by_month_and_weekday
        JOIN  cte_separed_by_month_and_weekday_with_total
        ON cte_separed_by_month_and_weekday.`month` = cte_separed_by_month_and_weekday_with_total.`month`
    ), cte_pivot_table_by_weekday AS (
		SELECT MONTHNAME(STR_TO_DATE(cte_joinded_total_and_week.`month`, '%m')) AS `month`, 
			SUM(IF(`weekday`= 0, avg_weekday_growth_up, NULL)) AS `Monday`,
            SUM(IF(`weekday`= 1, avg_weekday_growth_up, NULL)) AS `Thuesday`,
            SUM(IF(`weekday`= 2, avg_weekday_growth_up, NULL)) AS `Wednesday`,
            SUM(IF(`weekday`= 3, avg_weekday_growth_up, NULL)) AS `Thursday`,
            SUM(IF(`weekday`= 4, avg_weekday_growth_up, NULL)) AS `Friday`,
            #SUM(IF(`weekday`= 5, avg_weekday_growth_up, NULL)) AS `Saturday`,
			SUM(IF(`weekday`= 6, avg_weekday_growth_up, NULL)) AS `Sunday`,
			cte_joinded_total_and_week.`total`
			FROM cte_joinded_total_and_week
			GROUP BY cte_joinded_total_and_week.`month`
    )
    SELECT * FROM cte_pivot_table_by_weekday;
END $$ 

DELIMITER ;

CALL usp_get_week_growth_up_open_close();

#SELECT MONTHNAME(STR_TO_DATE(1,"%m"))
