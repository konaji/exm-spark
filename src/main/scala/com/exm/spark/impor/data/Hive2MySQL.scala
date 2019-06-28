package com.exm.spark.impor.data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Hive2MySQL {

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Hive2MySQL")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext

    // 读取hive数据
    val personDataFrame: DataFrame = spark.sql("select * from person")

    // 写入mysql
    personDataFrame.write.mode(SaveMode.Append).format("jdbc")
      .option("url", "jdbc:mysql://192.168.211.144:3306/sino")
      .option("dbtable", "hive2mysql")
      .option("user", "root")
      .option("password", "root")
      .save()

    sparkContext.stop
  }


}
