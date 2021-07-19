package org.example

import java.io.{File, FileInputStream}
import java.security.{DigestInputStream, MessageDigest}
import java.util.Date
import java.text.SimpleDateFormat
import configuration.{data_path, tmp_data_path}
import org.example.mysql_connection.{fill_audit_table, load_to_mysql_table, run_ddl_or_dml_operation, setup_db, truncate_all_tables}

import scala.collection.mutable.ArrayBuffer

object main extends App {
  def parse_arguments(): (String, String, String) = {
    var calculation_mode = "oc"
    var calculation_tool = "rdd"
    var load_mode = "incr"
    for (args_counter <- args.indices) {
      if (args(args_counter) == "--calculation_mode") {
        calculation_mode = args(args_counter + 1)
      }
      if (args(args_counter) == "--calculation_tool") {
        calculation_tool = args(args_counter + 1)
      }
      if (args(args_counter) == "--load_mode") {
        load_mode = args(args_counter + 1)
      }
    }
    return (calculation_mode, calculation_tool, load_mode)
  }

  def get_file_hash_sum(path: String): String = {
    val buffer = new Array[Byte](8192)
    val sha256 = MessageDigest.getInstance("SHA-256")
    val dis = new DigestInputStream(new FileInputStream(new File(path)), sha256)
    try {
      while (dis.read(buffer) != -1) {}
    } finally {
      dis.close()
    }
    sha256.digest.map("%02x".format(_)).mkString
  }

  def get_list_of_files_in_directory(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .map(_.getPath).toList
  }

  setup_db()
  val (calculation_mode, calculation_tool,load_mode) = parse_arguments()
  if (load_mode == "full"){
    truncate_all_tables()
  }
  val files_to_load = fill_audit_table(calculation_mode, calculation_tool)
  if (files_to_load != "") {
    val startTimeMillis = System.currentTimeMillis()
    calculation_tool match {
      case "rdd" => spark_rdd.start_calculation(calculation_mode, files_to_load)
      case "sql" => spark_sql.start_calculation(calculation_mode, files_to_load)
      case "dfr" => spark_dataframe.start_calculation(calculation_mode, files_to_load)
      case _ => println("Invalid tool")
    }

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Duration s:", durationSeconds)
  } else {
    println("Not found new files to load")
  }
}
