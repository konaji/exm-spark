package com.exm.spark.impor.data

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

object ES2MySQL {

  def main(args: Array[String]): Unit = {
    //SparkSession
    val sparkSession: SparkSession = SparkSession.builder().appName("ES2MySQL").master("local[1]").getOrCreate()
    val personRdd: RDD[String] = sparkSession.sparkContext.textFile("D:\\person.txt")
    import sparkSession.implicits._
    val df: DataFrame = personRdd.toDF()
    df.printSchema()
  }


  def test: Unit = {


    //配置spark与es连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ES2Mysql")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "192.168.211.144")
      .set("es.port", "9200")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //从es中index为test中查找内容为1的所有数据
    val rdd: RDD[(String, String)] = EsSpark.esJsonRDD(spark.sparkContext, "test", "?q=*1*")

    //将rdd转换成df
    import spark.implicits._
    val ds: Dataset[String] = rdd.map(_._2).toDS()
    val df: DataFrame = spark.read.json(ds.rdd)

    //将数据导入到mysql，其中test数据库必须存在，但是test表可以不存在，会自动创建表
    val jdbcUrl = "jdbc:mysql://192.168.211.144:3306/test?useSSL=false&characterEncoding=UTF-8"
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    df.write.mode(SaveMode.Append).jdbc(jdbcUrl,"test", prop)

    rdd.unpersist()
    spark.stop()
  }
}
