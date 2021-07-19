package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import configuration.{data_path, destination_data_path, stg_data_path, tmp_data_path}
import org.example.mysql_connection.{add_end_time_load_to_audit_table, incremental_load_from_tmp_to_stg_mysql_table, load_data_to_audit_tmp_to_stg_table, load_to_mysql_table}

object spark_sql extends App {
  def start_calculation(calculation_mode: String, files_to_load: String): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark_sql")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "571859200")
      .config("spark.executor.heartbeatInterval", "10000000")
      .config("spark.network.timeout", "10000000")
      .getOrCreate();

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val df_csv_file = spark.read.format("csv").option("header", "true").option("delimiter", ";")
      .option("encoding", "utf-8").csv(files_to_load.split(","):_*) //XAUUSD_10Secs_Ask_2020.01.01_2020.07.17.csv

    add_end_time_load_to_audit_table()
    /** Time (UTC)         |Open  |High  |Low   |Close |Volume |
     * +-------------------+------+------+------+------+------+
     * |2012-01-16 00:00:00|1291,4|1291,5|1291,4|1291,4|0     |
     * |2012-01-16 00:00:10|1291,4|1291,4|1291,4|1291,4|0     |
     */

    df_csv_file.createOrReplaceTempView("df_csv_file")
    val df_normalized = spark.sql("SELECT `Time (UTC)` AS time, DATE_FORMAT(`Time (UTC)`, 'y') AS year," +
      " CAST(DATE_FORMAT(`Time (UTC)`, 'M') AS INTEGER) AS month, " +
      " open," +
      " close" +
      " FROM df_csv_file")

    /** |               time|year|month|  open| close|
     * +-------------------+----+-----+------+------+
     * |2012-01-16 00:00:00|2012|    1|1291,4|1291,4|
     * |2012-01-16 00:00:10|2012|    1|1291,4|1291,4|
     */

    df_normalized.createOrReplaceTempView("df_normalized")
    val df_open_close_month_values = spark.sql("SELECT DISTINCT year, month," +
      " regexp_replace(FIRST_VALUE(open) OVER(PARTITION BY year, month ORDER BY time), ',', '.') AS open," +
      " regexp_replace(FIRST_VALUE(close) OVER(PARTITION BY year, month ORDER BY time DESC), ',', '.') as close" + // frame_clause? DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      " FROM df_normalized")

    /** |year|month|    open|   close|
     * +----+-----+--------+--------+
     * |2017|    7|2466.631|2467.431|
     * |2014|    5|1919.119|1919.402|
     */

    df_open_close_month_values.createOrReplaceTempView("df_open_close_month_values")
    val df_prev_close_value = spark.sql("SELECT *, COALESCE(LAG(close) OVER(ORDER BY year, month), open) AS prev_close " +
      "FROM df_open_close_month_values")

    /** |year|month|    open|   close|prev_close|
     * +----+-----+--------+--------+----------+
     * |2012|    1|  1291.4|  1314.1|    1291.4|
     * |2012|    8| 1409.75| 1409.64|    1314.1|
     */

    df_prev_close_value.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", true)
      .option("delimiter", ";")
      .csv(tmp_data_path)

    load_to_mysql_table("spark_audit_database.spark_tmp_table", tmp_data_path)
    load_data_to_audit_tmp_to_stg_table("sql")
    incremental_load_from_tmp_to_stg_mysql_table()

    spark.udf.register("get_calculation_mode", () => {
      calculation_mode
    })

    val df_full_data_from_mysql_csv = spark.read.format("csv").option("header", "true").option("delimiter", ";")
      .option("encoding", "utf-8").option("path", stg_data_path) // XAUUSD_10Secs_Ask_2020.01.01_2020.07.17.csv
      .load()

    df_prev_close_value.createOrReplaceTempView("df_full_data_from_mysql_csv")
    val df_growth_rate = spark.sql(" SELECT year, month, ROUND((close/(IF( get_calculation_mode() == 'oc',open,prev_close)) - 1) * 100, 2) AS growth_rate FROM df_full_data_from_mysql_csv")

    /** year|month|growth_rate|
     * +----+-----+-----------+
     * |2012|    1|       1.76|
     * |2012|    8|      -0.01|
     */

    df_growth_rate.createOrReplaceTempView("df_growth_rate")
    val df_pivot_by_month = spark.sql("SELECT * FROM df_growth_rate PIVOT (SUM(growth_rate) FOR month IN (1,2,3,4,5,6,7,8,9,10,11,12))")

    /** |year|   1|   2|   3|   4|    5|   6|   7|    8|   9|   10|   11|   12|
     * +----+----+----+----+----+-----+----+----+-----+----+-----+-----+-----+
     * |2012|1.76|null|null|null| null|null|null|-0.01|null|-0.04| null|  0.0|
     * |2013| 0.0|null|null|null|  0.0|null|null|-0.02|null|  0.0| null|26.68|
     */

    df_pivot_by_month.createOrReplaceTempView("df_pivot_by_month")
    val df_result = spark.sql("SELECT COALESCE(year, 'AVG') as year," +
      " ROUND(AVG(`1`), 2) AS Jan," +
      " ROUND(AVG(`2`), 2) AS Feb," +
      " ROUND(AVG(`3`), 2) AS Mar," +
      " ROUND(AVG(`4`), 2) AS Apr," +
      " ROUND(AVG(`5`), 2) AS May," +
      " ROUND(AVG(`6`), 2) AS Jun," +
      " ROUND(AVG(`7`), 2) AS Jul," +
      " ROUND(AVG(`8`), 2) AS Aug," +
      " ROUND(AVG(`9`), 2) AS Sep," +
      " ROUND(AVG(`10`), 2) AS Oct," +
      " ROUND(AVG(`11`), 2) AS Nov," +
      " ROUND(AVG(`12`), 2) AS Dec," +
      " ROUND(AVG(`1` + `2` + `3` + `4` + `5` + `6` + `7` + `8` + `9` + `10` + `11` + `12`) / 12, 2) AS AVG" +
      " FROM df_pivot_by_month" +
      " GROUP BY year WITH ROLLUP" +
      " ORDER by year")

    /** |year|  Jan|  Feb|  Mar|  Apr|  May|   Jun|  Jul|  Aug|  Sep|  Oct|  Nov|   Dec|  AVG|
     * +----+-----+-----+-----+-----+-----+------+-----+-----+-----+-----+-----+------+-----+
     * |2012|10.58|-1.91|-2.02|-0.09|-6.37|  2.38| 0.87| 4.93| 4.39|-2.56|-0.31| -3.44| 0.54|
     * |2013|-0.74|-4.93| 0.99|-7.53|-5.87|-10.78|  7.0| 5.24|-4.69|-0.41|-5.43|-26.73|-4.49|
     * |2014| 3.17|  6.7| -3.6| 0.56|-3.25|  6.07|-3.27| 0.29|-6.11| -2.9|-1.88| -1.48|-0.47|
     * |2015| 8.33|-5.37|-2.49| 0.07| 0.58| -1.56|-6.54| 3.55|-1.73| 2.41|-6.78| -9.91|-1.62|
     * |2016| 5.18|10.98| -0.7| 5.13|-5.76|  8.63| 2.06|-2.92| 0.51|-3.21|-8.12|  7.08| 1.57|
     * |2017| 5.24| 2.84| 0.27| 1.67|-0.13| -2.08| 2.08| 4.24|-3.09|-0.79| 0.39|  2.19| 1.07|
     * |2018| 3.23|-2.07| 0.66|-0.83| -1.2| -3.58|-2.24|-1.86|-0.81| 2.11| 0.53|  4.72|-0.11|
     * |2019|  3.0|-0.52|-1.71|-0.66| 1.75|  6.48| 1.44| 7.68|-3.59| 2.62|-3.19| 18.05| 2.61|
     * |2020| 4.64|-0.47|-0.76| 6.87| 3.06|  2.47| 1.55| null| null| null| null|  0.39| null|
     * | AVG| 4.74| 0.58|-1.04| 0.58|-1.91|  0.89| 0.33| 2.64|-1.89|-0.34| -3.1| -1.01|-0.11|
     */

    df_result.show(100)

    df_result.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", true)
      .option("delimiter", ";")
      .csv(destination_data_path)

    load_to_mysql_table("spark_audit_database.spark_destination_table", destination_data_path)
  }
}
