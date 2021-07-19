package org.example

import java.sql.{Connection, DriverManager}
import configuration._
import org.example.main.{get_file_hash_sum, get_list_of_files_in_directory}

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.mutable.ArrayBuffer

object mysql_connection extends App {
  var start_time_of_last_loaded_files: String = null

  def run_ddl_or_dml_operation(query: String): Unit = {
    var connection: Connection = null
    try {
      Class.forName(driver)
      val props = new Properties();
      props.setProperty("allowLoadLocalInfile", "true");
      props.setProperty("user", username);
      props.setProperty("password", password);
      connection = DriverManager.getConnection(url, props)
      val statement = connection.createStatement
      statement.executeUpdate(query)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    connection.close()
  }

  def run_select_operation(query: String, is_return_result: Boolean): ArrayBuffer[(String, String)] = {
    var connection: Connection = null
    val result = ArrayBuffer.empty[(String, String)]
    try {
      Class.forName(driver)
      val props = new Properties();
      props.setProperty("allowLoadLocalInfile", "true");
      props.setProperty("user", username);
      props.setProperty("password", password);
      connection = DriverManager.getConnection(url, props)
      val statement = connection.createStatement
      val rs = statement.executeQuery(query)
      if (is_return_result) {
        while (rs.next) {
          result.append((rs.getString("hash_sum"), rs.getString("end_timestamp")))
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    }
    connection.close()
    return result
  }

  def load_to_mysql_table(table_name: String, csv_path: String): Unit = {
    val truncate_table_sql_query = f"TRUNCATE TABLE $table_name%s"
    run_ddl_or_dml_operation(truncate_table_sql_query)
    val tmp_file = new File(csv_path).listFiles().filter(_.isFile).filter(_.getName.endsWith(".csv")).map(_.getPath).toList
    val path = tmp_file.head.replace("\\", "/")
    val load_data_sql_query = f"LOAD DATA LOCAL INFILE \'$path%s\' " +
      f" INTO TABLE $table_name%s " +
      " FIELDS TERMINATED BY ';'" +
      //" ENCLOSED BY '\"'" +
      " LINES TERMINATED BY '\n' " +
      " IGNORE 1 LINES"

    run_ddl_or_dml_operation(load_data_sql_query)
  }

  def incremental_load_from_tmp_to_stg_mysql_table(): Unit = {
    val update_data_sql_query = "REPLACE INTO spark_audit_database.spark_stg_table" +
      " (`year`, `month`,`open`, `close`,`prev_close`)" +
      " SELECT * FROM  spark_audit_database.spark_tmp_table"
    run_ddl_or_dml_operation(update_data_sql_query)
    val clean_path = stg_data_path.replace("\\", "/")
    new File(stg_data_path).delete()
    val save_updated_data_in_csv_query = "SELECT 'year', 'month', 'open', 'close', 'prev_close'" +
      " UNION" +
      " SELECT * FROM spark_audit_database.spark_stg_table" +
      f" LOCAL INTO OUTFILE \'$clean_path%s\'" +
      " FIELDS TERMINATED BY ';'" +
      "LINES TERMINATED BY '\n';"
    run_select_operation(save_updated_data_in_csv_query, false)
  }

  def load_data_to_audit_tmp_to_stg_table(calculation_tool: String): Unit = {
    val current_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    val fill_tmp_stg_audit_table_sql_query = "INSERT INTO spark_audit_database.spark_audit_table_tmp_to_stg " +
      f"SELECT *, '$current_time%s', '$calculation_tool%s' FROM spark_audit_database.spark_tmp_table"
    run_ddl_or_dml_operation(fill_tmp_stg_audit_table_sql_query)
  }

  def fill_audit_table(calculation_mode: String, calculation_tool: String): String = {
    val current_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    start_time_of_last_loaded_files = current_time
    val files_to_incremental_load = ArrayBuffer.empty[String]
    val list_of_audit_files_and_end_timestamp = run_select_operation("SELECT `hash_sum`, `end_timestamp`" +
      " FROM spark_audit_database.spark_audit_table", true)
    val checksum_and_end_timestamp = collection.mutable.Map[String, String]()
    for (value <- list_of_audit_files_and_end_timestamp) {
      checksum_and_end_timestamp(value._1) = value._2
    }
    for (file_path <- get_list_of_files_in_directory(data_path)) {
      val hash_sum = get_file_hash_sum(file_path)
      if (checksum_and_end_timestamp.contains(hash_sum)
        && !checksum_and_end_timestamp.get(hash_sum).contains(null)) {
        //println(checksum_and_end_timestamp.get(get_file_hash_sum(file_path)))
        println(f"$file_path%s was processed")
      } else {
        if ((checksum_and_end_timestamp.contains(hash_sum)
          && checksum_and_end_timestamp.get(hash_sum).contains(null))
          || !checksum_and_end_timestamp.contains(hash_sum)) {
          files_to_incremental_load.append(file_path)
          val csv_source = scala.io.Source.fromFile(file_path)
          val lines_in_file = csv_source.getLines().size
          csv_source.close()
          run_ddl_or_dml_operation(f"INSERT INTO spark_audit_database.spark_audit_table " +
            f"VALUES('$hash_sum%s', '$current_time%s',' ${file_path.replace("\\", "/")}%s', '$calculation_tool%s', '$lines_in_file%s', null)")
        }
      }
    }
    return files_to_incremental_load.mkString(",").replace("\\", "/")
  }

  def setup_db(): Unit = {
    run_ddl_or_dml_operation("SET GLOBAL time_zone = '+0:00'")
    run_ddl_or_dml_operation("SET GLOBAL local_infile=1")
    val create_db_sql_query = "CREATE DATABASE IF NOT EXISTS spark_audit_database"
    val create_spark_audit_table_sql_query = "CREATE TABLE IF NOT EXISTS spark_audit_database.spark_audit_table (`hash_sum` VARCHAR(64)" +
      ", `start_timestamp` TIMESTAMP, `file_name` TEXT, `calculation_tool` VARCHAR(3), `lines_in_file` BIGINT, `end_timestamp` TIMESTAMP)"
    val create_spark_audit_table_tmp_to_stg_sql_query = "CREATE TABLE IF NOT EXISTS spark_audit_database.spark_audit_table_tmp_to_stg" +
      " (`year` INT , `month` INT, `open` DOUBLE, `close` DOUBLE , `prev_close` DOUBLE, load_timestamp TIMESTAMP, `calculation_tool` VARCHAR(3))"
    val create_spark_stg_table_sql_query = "CREATE TABLE IF NOT EXISTS spark_audit_database.spark_stg_table" +
      " (`year` INT , `month` INT, `open` DOUBLE, `close` DOUBLE , `prev_close` DOUBLE, UNIQUE KEY `year_month` (`year`,`month`))"
    val create_spark_tmp_table_sql_query = "CREATE TABLE IF NOT EXISTS spark_audit_database.spark_tmp_table" +
      " (`year` INT , `month` INT, `open` DOUBLE, `close` DOUBLE , `prev_close` DOUBLE)"
    val create_spark_destination_table = "CREATE TABLE IF NOT EXISTS spark_audit_database.spark_destination_table " +
      " (`year` VARCHAR(4), `Jan` DOUBLE, `Feb` DOUBLE, `Mar` DOUBLE,`Apr` DOUBLE, `May` DOUBLE, `Jun` DOUBLE, `Jul` DOUBLE," +
      " `Aug` DOUBLE, `Sep` DOUBLE, `Oct` DOUBLE, `Nov` DOUBLE, `Dec` DOUBLE, `AVG` DOUBLE)"

    run_ddl_or_dml_operation(create_db_sql_query)
    run_ddl_or_dml_operation(create_spark_audit_table_sql_query)
    run_ddl_or_dml_operation(create_spark_audit_table_tmp_to_stg_sql_query)
    run_ddl_or_dml_operation(create_spark_stg_table_sql_query)
    run_ddl_or_dml_operation(create_spark_tmp_table_sql_query)
    run_ddl_or_dml_operation(create_spark_destination_table)
  }

  def truncate_all_tables(): Unit = {
    val truncate_audit_table_sql_query = "TRUNCATE TABLE spark_audit_database.spark_audit_table"
    val truncate_audit_table_tmp_to_stg_sql_query = "TRUNCATE TABLE spark_audit_database.spark_audit_table_tmp_to_stg"
    val truncate_destination_table_sql_query = "TRUNCATE TABLE spark_audit_database.spark_stg_table"
    run_ddl_or_dml_operation(truncate_audit_table_sql_query)
    run_ddl_or_dml_operation(truncate_audit_table_tmp_to_stg_sql_query)
    run_ddl_or_dml_operation(truncate_destination_table_sql_query)
  }

  def add_end_time_load_to_audit_table(): Unit = {
    val current_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    val update_end_time_sql_query = f"UPDATE spark_audit_database.spark_audit_table SET `end_timestamp`='$current_time%s' " +
      f" WHERE start_timestamp = '$start_time_of_last_loaded_files%s'"
    run_ddl_or_dml_operation(update_end_time_sql_query)
  }
}
