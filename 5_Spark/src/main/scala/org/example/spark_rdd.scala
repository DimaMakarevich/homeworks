package org.example

import org.apache.commons.math3.util.Precision.round
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, rdd}
import configuration.{data_path, destination_data_path, stg_data_path, tmp_data_path}
import org.example.mysql_connection.{add_end_time_load_to_audit_table, incremental_load_from_tmp_to_stg_mysql_table, load_data_to_audit_tmp_to_stg_table, load_to_mysql_table}

object spark_rdd extends App {
  def start_calculation(calculation_mode: String, files_to_load: String): Unit = {
    def normalize_rdd_csv(line: String): (String, (String, String, String)) = {
      if (line == "Time (UTC);Open;High;Low;Close;Volume ") {
        return ("NULL", ("NULL", "NULL", "NULL"))
      } else {
        val tokenized_line = line.split(";")
        val (full_date, year_month, open, close) = (tokenized_line(0)
          , tokenized_line(0).slice(0, 7)
          , tokenized_line(1).replace(",", ".")
          , tokenized_line(4).replace(",", ".")
        )
        return (year_month, (full_date, open, close))
      }
    }

    def find_max_and_min_month_value(line: (String, Iterable[(String, String, String)]))
    : (String, ((String, String, String), (String, String, String))) = {
      var last_month_day: (String, String, String) = ("", "", "")
      var first_month_day: (String, String, String) = ("", "", "")
      val values_list = line._2
      for (element <- values_list) {
        if (last_month_day._1 < element._1) {
          last_month_day = element
        }

        if (first_month_day._1 > element._1 || first_month_day._1 == "") {
          first_month_day = element

        }
      }
      return ("Key", (first_month_day, last_month_day))
    }

    def add_prev_close(line: (String, Iterable[((String, String, String), (String, String, String))])):
    Iterable[(String, (String, String, String))] = {
      var array_list = line._2.toArray
      array_list = array_list.sortWith(_._1._1 < _._1._1)
      var prev_close_value: String = ""
      val array_with_prev_close = scala.collection.mutable.ArrayBuffer.empty[(String, (String, String, String))]
      for (element <- array_list) {
        if (prev_close_value == "") {
          prev_close_value = element._1._2
        }
        val year_and_month = element._2._1.slice(0, 7)
        array_with_prev_close.append((year_and_month, (element._1._2, element._2._3, prev_close_value)))
        prev_close_value = element._2._3
      }
      return array_with_prev_close
    }

    def get_growth_rate_with_calculation_mode(line: (String, (String, String, String))): (String, (Double, String)) = {
      val (year, month) = (line._1.slice(0, 4), line._1.slice(5, 7))
      if (calculation_mode == "oc") {
        val growth_rate = (line._2._2.toDouble / line._2._1.toDouble - 1) * 100
        return (year, (growth_rate, month))
      } else {
        val growth_rate = (line._2._2.toDouble / line._2._3.toDouble - 1) * 100
        return (year, (growth_rate, month))
      }
    }

    def calculate_average_column(line: (String, Iterable[(Double, String)])): (String, List[Double]) = {
      val array_list = line._2
      val growth_rate_array = new Array[Double](13);
      var total_growth_rate: Double = 0
      var month_counter = 0
      for (growth_rate_and_month <- array_list) {
        growth_rate_array(growth_rate_and_month._2.toInt - 1) = growth_rate_and_month._1
        total_growth_rate += growth_rate_and_month._1
        month_counter += 1
      }
      growth_rate_array(12) = total_growth_rate / month_counter
      return (line._1, growth_rate_array.toList)
    }

    def transform_to_total_row(line: (String, (Double, String))): (String, (Double, Int)) = {
      val (month, growth_rate) = (line._2._2, line._2._1)
      return (month, (growth_rate, 1))
    }

    def reduce_average_month_growth_rate(left_arg: (Double, Int), right_argument: (Double, Int)): (Double, Int) = {
      return ((left_arg._1 + right_argument._1, left_arg._2 + right_argument._2))
    }

    def calculate_average_month_growth_rate(line: (String, (Double, Int))): (String, Double) = {
      val average_value = line._2._1 / line._2._2
      val month_number = line._1
      return (month_number, average_value)
    }

    def calculate_average_row(line: (Boolean, Iterable[(String, Double)])): (String, List[Double]) = {
      val growth_rate_array = new Array[Double](13);
      val array_list = line._2
      var total_growth_rate: Double = 0
      var month_counter = 0
      for (month_and_growth_rate <- array_list) {
        growth_rate_array(month_and_growth_rate._1.toInt - 1) = month_and_growth_rate._2
        total_growth_rate += month_and_growth_rate._2
        month_counter += 1
      }
      growth_rate_array(12) = total_growth_rate / month_counter
      return ("AVG", growth_rate_array.toList)
    }

    def convert_list_value_to_tuple(list: (String, List[Double])): (String, (String, String, String, String, String, String
      , String, String, String, String, String, String, String)) = {
      val values_tuple = (round(list._2(0), 2).toString, round(list._2(1), 2).toString, round(list._2(2), 2).toString
        , round(list._2(3), 2).toString, round(list._2(4), 2).toString, round(list._2(5), 2).toString
        , round(list._2(6), 2).toString, round(list._2(7), 2).toString, round(list._2(8), 2).toString
        , round(list._2(9), 2).toString, round(list._2(10), 2).toString, round(list._2(11), 2).toString
        , round(list._2(12), 2).toString)
      val year = list._1
      return (year, values_tuple)
    }

    def normalize_csv_from_mysql(line: String): (String, (String, String, String)) = {
      val tokenized_line = line.split(";")
      val (year, month, open, close, prev_close) = (tokenized_line(0), tokenized_line(1), tokenized_line(2), tokenized_line(3), tokenized_line(4))
      return (year.concat("-").concat(month), (open, close, prev_close))
    }

    //conf.set()
    // val config = SparkConf

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("spark_rdd")
      .config("spark.testing.memory", "2147480000")
      .config("spark.driver.memory", "571859200")
      .config("spark.executor.heartbeatInterval", "10000000")
      .config("spark.network.timeout", "10000000")
      .getOrCreate();
    // 471859200
    //spark.conf.set("spark.testing.memory", "2g")
    //spark.conf.set("spark.driver.memory", "4g")

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    add_end_time_load_to_audit_table()

    val rdd_csv = sc.textFile(files_to_load)


    /** Time (UTC);Open;High;Low;Close;Volume
     * 2012-01-16 00:00:00;1291,4;1291,5;1291,4;1291,4;0
     */

    val rdd_normalized = rdd_csv.map(normalize_rdd_csv).filter(line => {
      line._1 != "NULL"
    })

    /** (2012-01,(2012-01-16 00:00:00,1291.4,1291.4))
     * (2012-01,(2012-01-16 00:00:10,1291.4,1291.4))
     */

    val rdd_grouped_by_year_month = rdd_normalized.groupByKey()

    /** (2015-05,CompactBuffer((2015-05-21 05:03:40,2125.828,2125.828), (2015-05-21 05:03:50,2125.828,2125.828), ...
     * (2015-12,CompactBuffer((2015-12-31 23:51:30,2058.134,2058.134), ...
     * */

    val rdd_max_and_min_month_value = rdd_grouped_by_year_month.map(find_max_and_min_month_value)

    /** (Key,((2015-05-21 05:03:40,2125.828,2125.828),(2015-05-21 05:07:10,2125.828,2125.828)))
     * (Key,((2015-12-31 23:51:30,2058.134,2058.134),(2015-12-31 23:59:00,2058.134,2058.134)))
     * */

    val rdd_in_one_group = rdd_max_and_min_month_value.groupByKey()

    /** (Key,CompactBuffer(((2015-05-21 05:03:40,2125.828,2125.828),(2015-05-21 05:07:10,2125.828,2125.828)), ....
     */

    val rdd_with_prev_close = rdd_in_one_group.flatMap(add_prev_close)

    /** (2012-01,(1291.4,1314.1,1291.4))
     * (2012-08,(1409.75,1409.64,1314.1))
     */
    val rdd_to_write = rdd_with_prev_close.map(tuple => {
      (tuple._1.slice(0, 4), tuple._1.slice(5, 7), tuple._2._1, tuple._2._2, tuple._2._3)
    })

    /** (2012,01,1291.4,1314.1,1291.4)
     * (2012,08,1409.75,1409.64,1314.1)
     */

    spark.createDataFrame(rdd_to_write).toDF("year", "month", "open", "close", "prev_close")
      .repartition(1)
      .write.mode(SaveMode.Overwrite)
      .option("header", true)
      .option("delimiter", ";")
      .csv(tmp_data_path)

    load_to_mysql_table("spark_audit_database.spark_tmp_table", tmp_data_path)
    load_data_to_audit_tmp_to_stg_table("rdd")
    incremental_load_from_tmp_to_stg_mysql_table()

    val rdd_full_data_from_mysql_csv = sc.textFile(stg_data_path).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => {
        line
      })

    /** (2012,01,1291.4,1314.1,1291.4)
     * (2012,08,1409.75,1409.64,1314.1)
     */

    val rdd_tokenized_from_mysql = rdd_full_data_from_mysql_csv.map(normalize_csv_from_mysql)

    /** (2012-01,(1291.4,1314.1,1291.4))
     * (2012-08,(1409.75,1409.64,1314.1))
     */

    val rdd_growth_rate = rdd_tokenized_from_mysql.map(get_growth_rate_with_calculation_mode)

    /** (2012,(1.7577822518197062,01))
     * (2012,(-0.007802801915224311,08))
     */

    val rdd_growth_rate_group_by_year = rdd_growth_rate.groupByKey()

    /** (2015,CompactBuffer((0.0,01), (0.0,05), (0.0,10), (0.0,12)))
     * (2017,CompactBuffer((0.0,01), (0.03243290139465227,07), (-0.03246677045601887,10), (0.0,12)))
     */

    val rdd_with_average_column = rdd_growth_rate_group_by_year.map(calculate_average_column)

    /** (2015,List(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
     * (2017,List(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.03243290139465227, 0.0, 0.0, -0.03246677045601887, 0.0, 0.0, -8.467265341649544E-6))
     */

    val rdd_with_month_key = rdd_growth_rate.map(transform_to_total_row)

    /** (01,1.7577822518197062,1)
     * (08,-0.007802801915224311,1)
     */

    val rdd_reduced_by_month_key = rdd_with_month_key.reduceByKey(reduce_average_month_growth_rate)

    /** (11,(5.770155850442505,2))
     * (08,(-0.025532020021257207,2))
     */

    val rdd_calculated_average_in_month_growth_rate = rdd_reduced_by_month_key.map(calculate_average_month_growth_rate)

    /** (11,2.8850779252212524)
     * (08,-0.012766010010628603)
     */

    val rdd_avg_int_one_group = rdd_calculated_average_in_month_growth_rate.groupBy(line => {
      true
    })

    /** (true,CompactBuffer((11,2.8850779252212524), (08,-0.012766010010628603), (09,-4.196346290175068E-4),...
     */

    val rdd_average_row = rdd_avg_int_one_group.map(calculate_average_row)

    /** (AVG,List(0.251111750259958, 0.0, 0.0, 0.0, 0.002717806213423657, 0.0, 0.03243290139465227, -0.012766010010628603,....), ...)
     */

    val rd_avg_column_and_row = sc.union(rdd_with_average_column, rdd_average_row).map(convert_list_value_to_tuple)

    /** (2015,(0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0))
     * (2017,(0.0,0.0,0.0,0.0,0.0,0.0,0.03,0.0,0.0,-0.03,0.0,0.0,-0.0))
     */

    val rdd_column_name = sc.parallelize(Seq(("!Year", ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "AVG"))))

    /** (!Year,(Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,AVG))
     */

    val rdd_result = sc.union(rd_avg_column_and_row, rdd_column_name).sortByKey()

    /** (!Year,(Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,AVG))
     * (2012,(1.76,0.0,0.0,0.0,0.0,0.0,0.0,-0.01,0.0,-0.04,0.0,-0.03,0.42))
     * (2013,(0.0,0.0,0.0,0.0,0.0,0.0,0.0,-0.02,0.0,0.0,0.0,2.27,0.45))
     * (2014,(0.0,0.0,0.0,0.0,0.01,0.0,0.0,0.0,-0.0,0.0,0.0,-0.11,-0.02))
     * (2015,(0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0))
     * (2016,(0.0,0.0,0.0,0.0,0.01,0.0,0.0,0.0,0.0,0.0,5.78,0.0,1.45))
     * (2017,(0.0,0.0,0.0,0.0,0.0,0.0,0.03,0.0,0.0,-0.03,0.0,0.0,-0.0))
     * (2018,(0.0,0.0,0.0,0.0,-0.01,0.0,0.0,0.0,-0.0,0.0,-0.01,0.0,-0.0))
     * (AVG,(0.25,0.0,0.0,0.0,0.0,0.0,0.03,-0.01,-0.0,-0.02,2.89,0.3,0.43))
     */

    rdd_result.take(20).foreach(line => {
      println(line)
    })

    val rdd_to_write_result = rdd_result.map(tuple => {
      (tuple._1, tuple._2._1, tuple._2._2, tuple._2._3, tuple._2._4, tuple._2._5, tuple._2._6, tuple._2._7, tuple._2._8
        , tuple._2._9, tuple._2._10, tuple._2._11, tuple._2._12, tuple._2._13)
    }).filter(tuple=>{tuple._1 != "!Year"})

    spark.createDataFrame(rdd_to_write_result).toDF("year", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul"
      , "Aug", "Sep", "Oct", "Nov", "Dec", "AVG").repartition(1).write.mode(SaveMode.Overwrite)
      .option("header", true)
      .option("delimiter", ";")
      .csv(destination_data_path)

    load_to_mysql_table("spark_audit_database.spark_destination_table", destination_data_path)
  }
}
//  (2012-01,(1570.057,1736.106,1570.057))
//  (2012-02,(1736.191,1702.999,1736.106))
//  (2012-03,(1703.018,1668.636,1702.999))
//  (2012-04,(1668.636,1667.141,1668.636))
//  (2012-05,(1667.17,1561.048,1667.141))
//  (2012-06,(1561.047,1598.135,1561.048))
//  (2012-07,(1599.657,1613.51,1598.135))
//  (2012-08,(1613.474,1693.016,1613.51))
//  (2012-09,(1691.617,1765.817,1693.016))
//  (2012-10,(1765.759,1720.554,1765.817))
//  (2012-11,(1720.537,1715.157,1720.554))
//  (2012-12,(1715.167,1675.627,1715.157))