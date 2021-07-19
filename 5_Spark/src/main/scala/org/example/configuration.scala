package org.example

object configuration {
  val url = "jdbc:mysql://localhost:3306/mysql?useLegacyDatetimeCode=false&serverTimezone=UTC"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "Dima1790426"
  val data_path="C:\\Users\\DmitryMakarevich\\Desktop\\homeworks\\5_Spark\\fund"
  val tmp_data_path = "C:\\Users\\DmitryMakarevich\\Desktop\\homeworks\\5_Spark\\csv_tables\\tmp_new_data_from_spark"
  val stg_data_path = "C:\\Users\\DmitryMakarevich\\Desktop\\homeworks\\5_Spark\\csv_tables\\stg_from_mysql\\stg.csv"
  val destination_data_path = "C:\\Users\\DmitryMakarevich\\Desktop\\homeworks\\5_Spark\\csv_tables\\destination_from_spark"
}
