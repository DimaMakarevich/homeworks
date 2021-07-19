package org.example

import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, date_format, expr, first, lag, last, lit, regexp_replace, round, to_timestamp, when}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}
import configuration.{data_path, destination_data_path, stg_data_path, tmp_data_path}
import org.example.mysql_connection.{add_end_time_load_to_audit_table, incremental_load_from_tmp_to_stg_mysql_table, load_data_to_audit_tmp_to_stg_table, load_to_mysql_table}

object spark_dataframe extends App {

  def start_calculation(calculation_mode: String, files_to_load: String): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark_dataframe")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "571859200")
      .config("spark.executor.heartbeatInterval", "10000000")
      .config("spark.network.timeout", "10000000")
      .getOrCreate();

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val df_csv_file = spark.read.format("csv").option("header", "true").option("delimiter", ";")
      .option("encoding", "utf-8").csv(files_to_load.split(","):_*) // XAUUSD_10Secs_Ask_2020.01.01_2020.07.17.csv
    add_end_time_load_to_audit_table()
    /** Time (UTC)         |Open  |High  |Low   |Close |Volume |
     * +-------------------+------+------+------+------+------+
     * |2012-01-16 00:00:00|1291,4|1291,5|1291,4|1291,4|0     |
     * |2012-01-16 00:00:10|1291,4|1291,4|1291,4|1291,4|0     |
     */

    val df_normalized = df_csv_file.select(to_timestamp(df_csv_file("Time (UTC)")).alias("time")
      , date_format(df_csv_file("Time (UTC)"), "y").alias("year")
      , date_format(df_csv_file("Time (UTC)"), "M").alias("month"), df_csv_file("open"), df_csv_file("close"))

    /** |               time|year|month|  open| close|
     * +-------------------+----+-----+------+------+
     * |2012-01-16 00:00:00|2012|    1|1291,4|1291,4|
     * |2012-01-16 00:00:10|2012|    1|1291,4|1291,4|
     */

    //val windowSpec_for_open = Window.partitionBy("year", "month").orderBy(col("time"))
    val windowSpec_for_close = Window.partitionBy("year", "month").orderBy(col("time")).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    val df_open_close_month_values = df_normalized.select(df_normalized("year"), df_normalized("month")
      , regexp_replace(first(df_normalized("open")).over(windowSpec_for_close), ",", ".").alias("open")
      , regexp_replace(last(df_normalized("close")).over(windowSpec_for_close), ",", ".").alias("close")).dropDuplicates("year", "month")
    // df_open_close_month_values.orderBy("year").show(200)
    /** |year|month|    open|   close|
     * +----+-----+--------+--------+
     * |2017|    7|2466.631|2467.431|
     * |2014|    5|1919.119|1919.402|
     */
    val df_cast_to_int_year_and_month = df_open_close_month_values.select(df_open_close_month_values("year").cast("Int")
      , df_open_close_month_values("month").cast("Int"), df_open_close_month_values("open"), df_open_close_month_values("close"))

    /** |year|month|    open|   close|
     * +----+-----+--------+--------+
     * |2017|    7|2466.631|2467.431|
     * |2014|    5|1919.119|1919.402|
     */

    val windowSpec_for_prev_close = Window.orderBy("year", "month")
    val df_prev_close_value = df_cast_to_int_year_and_month.select(df_cast_to_int_year_and_month("*")
      , coalesce(lag(df_cast_to_int_year_and_month("close"), 1).over(windowSpec_for_prev_close)
        , df_cast_to_int_year_and_month("open")).alias("prev_close"))

    /** |year|month|    open|   close|prev_close|
     * +----+-----+--------+--------+----------+
     * |2012|    1|  1291.4|  1314.1|    1291.4|
     * |2012|    8| 1409.5| 1409.64|    1314.1|
     */

    df_prev_close_value.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", true)
      .option("delimiter", ";")
      .csv(tmp_data_path)

    load_to_mysql_table("spark_audit_database.spark_tmp_table", tmp_data_path)
    load_data_to_audit_tmp_to_stg_table("dfr")
    incremental_load_from_tmp_to_stg_mysql_table()

    val df_full_data_from_mysql_csv = spark.read.format("csv").option("header", "true").option("delimiter", ";")
      .option("encoding", "utf-8").option("path", stg_data_path) // XAUUSD_10Secs_Ask_2020.01.01_2020.07.17.csv
      .load()

    /** |year|month|    open|   close|prev_close|
     * +----+-----+--------+--------+----------+
     * |2012|    1|  1291.4|  1314.1|    1291.4|
     * |2012|    8| 1409.5| 1409.64|    1314.1|
     */
    //load_to_current_table

    val df_growth_rate = df_full_data_from_mysql_csv.select(df_full_data_from_mysql_csv("year").cast("String"), df_full_data_from_mysql_csv("month")
      , round(when(lit(calculation_mode) === "oc"
        , (df_full_data_from_mysql_csv("close").cast("Double") / df_full_data_from_mysql_csv("open").cast("Double") - 1) * 100)
        .when(lit(calculation_mode) === "cc", (df_full_data_from_mysql_csv("close").cast("Double") / df_full_data_from_mysql_csv("prev_close")
          .cast("Double") - 1) * 100), 2).alias("growth_rate"))

    /** |year|month|growth_rate|
     * +----+-----+-----------+
     * |2012|   10|     -0.039|
     * |2017|   10|      -0.03|
     */

    val months = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
    val df_pivot_by_month = df_growth_rate.groupBy(df_growth_rate("year")).pivot(df_growth_rate("month"), months).sum("growth_rate")

    /** |year|   1|    5|   7|    8|   9|   10|   11|   12|
     * +----+----+-----+----+-----+----+-----+-----+-----+
     * |2018| 0.0|-0.01|null| null| 0.0| null|-0.01|-4.71|
     * |2015| 0.0|  0.0|null| null|null|  0.0| null|  0.0|
     */

    val df_avg_column = df_pivot_by_month.withColumn("AVG", df_pivot_by_month.columns
      .drop(1)
      .map(col)
      .reduce(_ + _)
      .divide(df_pivot_by_month.columns.length - 1))

    /** |year|   1|    5|   7|    8|   9|   10|   11|   12| AVG|
     * +----+----+-----+----+-----+----+-----+-----+-----+----+
     * |2018| 0.0|-0.01|null| null| 0.0| null|-0.01|-4.71|null|
     * |2015| 0.0|  0.0|null| null|null|  0.0| null|  0.0|null|
     */

    val df_avg_row = df_avg_column.rollup(df_avg_column("year")).avg()

    /** |year|             avg(1)|avg(5)|avg(7)|avg(8)|avg(9)|avg(10)|           avg(11)|           avg(12)|avg(AVG)|
     * +----+-------------------+------+------+------+------+-------+------------------+------------------+--------+
     * |null|0.25142857142857145| 0.002|  0.03|-0.015|   0.0|-0.0175|2.8850000000000002|7.2250000000000005|    null|
     * |2018|                0.0| -0.01|  null|  null|   0.0|   null|             -0.01|             -4.71|    null|
     */

    val df_result = df_avg_row.select(when(df_avg_row("year") <=> null, lit("AVG")).otherwise(df_avg_row("year")).alias("year")
      , round(df_avg_row("avg(1)"), 2).alias("Jan")
      , round(df_avg_row("avg(2)"), 2).alias("Feb")
      , round(df_avg_row("avg(3)"), 2).alias("Mar")
      , round(df_avg_row("avg(4)"), 2).alias("Apr")
      , round(df_avg_row("avg(5)"), 2).alias("May")
      , round(df_avg_row("avg(6)"), 2).alias("Jun")
      , round(df_avg_row("avg(7)"), 2).alias("Jul")
      , round(df_avg_row("avg(8)"), 2).alias("Aug")
      , round(df_avg_row("avg(9)"), 2).alias("Sep")
      , round(df_avg_row("avg(10)"), 2).alias("Oct")
      , round(df_avg_row("avg(11)"), 2).alias("Nov")
      , round(df_avg_row("avg(12)"), 2).alias("Dec")
      , round(df_avg_row("avg(AVG)"), 2).alias("AVG")).orderBy(df_avg_row("year"))

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

    df_result.show(20)

    df_result.repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", true)
      .option("delimiter", ";")
      .csv(destination_data_path)

    load_to_mysql_table("spark_audit_database.spark_destination_table", destination_data_path)
  }
}

/*
  val schema = StructType(Array(
    StructField("language", StringType, true),
    StructField("cost", StringType, true)
  ))
  val rowData = Seq(
    Row("2012", "100000"),
    Row("2013", "3000"),
    Row("AVG", "20000"))
  var dfFromData3 = spark.createDataFrame(rowData, schema)
  dfFromData3.orderBy(dfFromData3("language")).show(20)
  dfFromData3.printSchema()
*/