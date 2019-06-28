package com.exm.spark.impor.data

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 将List集合数据存入MySQL
  */
object List2MySQL {

  case class Blog (name:String,count:Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("List2MySQL")
      .master("local[1]")
      .getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext
    val data = sparkContext.makeRDD(List(("www", 10), ("iteblog", 20), ("com", 30)))

    import spark.implicits._
    val dataFrame = data.map(x => new Blog(x._1, x._2)).toDF()

    // 将dataFrame写入MySQL,如果表不存在,将自动创建
    dataFrame.write.mode(SaveMode.Append).format("jdbc")
      .option("url", "jdbc:mysql://192.168.211.144:3306/sino")
      .option("dbtable", "list")
      .option("user", "root")
      .option("password", "root")
      .save()
  }
}
