CREATE OR REPLACE PROCEDURE usp_etl_stg2_to_destination(calculate_mode VARCHAR(2))
BEGIN
    TRUNCATE TABLE forex_db.forex_destination;
    SET query='
    WITH cte_growth_rate AS (
        SELECT stg2.`time`, stg2.`year`, stg2.`month`, (stg2.`close`/(IF("' || calculate_mode || '"= "oc" ,stg2.`open`, stg2.`prev_close`)) - 1 ) * 100 AS `growth_rate`  FROM forex_db.forex_stg2 stg2
    ),cte_pivot AS (
        SELECT cte_g.`year`,
            SUM(IF(cte_g.`month`=1, cte_g.`growth_rate`, NULL)) AS `Jan`,
            SUM(IF(cte_g.`month`=2, cte_g.`growth_rate`, NULL)) AS `Feb`,
            SUM(IF(cte_g.`month`=3, cte_g.`growth_rate`, NULL)) AS `Mar`,
            SUM(IF(cte_g.`month`=4, cte_g.`growth_rate`, NULL)) AS `Apr`,
            SUM(IF(cte_g.`month`=5, cte_g.`growth_rate`, NULL)) AS `May`,
            SUM(IF(cte_g.`month`=6, cte_g.`growth_rate`, NULL)) AS `Jun`,
            SUM(IF(cte_g.`month`=7, cte_g.`growth_rate`, NULL)) AS `Jul`,
            SUM(IF(cte_g.`month`=8, cte_g.`growth_rate`, NULL)) AS `Aug`,
            SUM(IF(cte_g.`month`=9, cte_g.`growth_rate`, NULL)) AS `Sep`,
            SUM(IF(cte_g.`month`=10, cte_g.`growth_rate`, NULL)) AS `Oct`,
            SUM(IF(cte_g.`month`=11, cte_g.`growth_rate`, NULL)) AS `Nov`,
            SUM(IF(cte_g.`month`=12, cte_g.`growth_rate`, NULL)) AS `Dec`
            FROM cte_growth_rate cte_g
            GROUP BY cte_g.`year`
    ), cte_pivot_with_avg AS (
    SELECT COALESCE(cte_p.`year`, "Total") AS `year`, AVG(cte_p.`Jan`) AS `Jan`, AVG(cte_p.`Feb`) AS `Feb`, AVG(cte_p.`Mar`) AS `Mar` , AVG(cte_p.`Apr`) AS `Apr`,
                AVG(cte_p.`May`) AS `May`, AVG(cte_p.`Jun`) AS `Jun`, AVG(cte_p.`Jul`) AS `Jul`, AVG(cte_p.`Aug`) AS `Aug`, AVG(cte_p.`Sep`) AS `Sep`, AVG(cte_p.`Oct`) AS `Oct`,
                AVG(cte_p.`Nov`) AS `Nov`, AVG(cte_p.`Dec`) AS `Dec`,
                AVG(cte_p.`Jan` + cte_p.`Feb` + cte_p.`Mar` + cte_p.`Apr` + cte_p.`May` + cte_p.`Jun` + cte_p.`Jul` + cte_p.`Aug` + cte_p.`Sep` + cte_p.`Oct` + cte_p.`Nov` + cte_p.`Dec` ) AS `Total`
    FROM cte_pivot cte_p
    GROUP BY cte_p.`year` WITH ROLLUP 
    ORDER BY `year` 
    )
    FROM cte_pivot_with_avg cte_w
	INSERT INTO forex_db.forex_destination ( `year`, `jan`, `feb`, `mar`, `apr`, `may`, `jun`, `jul`, `aug`, `sep`, `oct`, `nov`, `dec`, `total`)
    SELECT cte_w.* 
    ';
     EXECUTE query;
END;
