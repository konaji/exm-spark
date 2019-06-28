package com.exm.spark.impor.data

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSQL")
      .master("local[1]")
      .enableHiveSupport()
      .getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext

    val personDataFrame: DataFrame = spark.sql("select * from person")
    /**
      * createOrReplaceTempView：创建临时视图，此视图的生命周期与用于创建此数据集的SparkSession相关联。
      *
      * createGlobalTempView：创建全局临时视图，此时图的生命周期与Spark Application绑定。
      *
      * 两者的区别就在于多个SparkSession中,能否在任意一个SparkSession中查询注册的表结果信息
      */

    /**
      * createTempView : 创建临时表，如果已存在同名表则报错。
      *
      * createOrReplaceTempView : 创建临时表，如果存在则进行替换，与老版本的registerTempTable功能相同。
      */
    //df.createOrReplaceTempView("dataBlock")
    //df.show(10)
    sparkContext.stop
  }
}
