package com.exm.spark.impor.data

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

object ES2MySQL {

  def main(args: Array[String]): Unit = {
    //esRDD
    esJsonRDD
  }


  def esJsonRDD: Unit = {
    //配置spark与es连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ES2Mysql")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "192.168.211.144")
      .set("es.port", "9200")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //从es中index为test中查找内容为1的所有数据
    val esJsonRDD: RDD[(String, String)] = EsSpark.esJsonRDD(spark.sparkContext, "sino_person", "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}")

    //将rdd转换成df
    import spark.implicits._
    val dataFrame: Dataset[String] = esJsonRDD.map(_._2).toDS()
    dataFrame.show()

    val df: DataFrame = spark.read.json(dataFrame.rdd)

    //将数据导入到mysql，其中test数据库必须存在，但是test表可以不存在，会自动创建表
    val jdbcUrl = "jdbc:mysql://192.168.211.144:3306/sino?useSSL=false&characterEncoding=UTF-8"
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    df.write.mode(SaveMode.Append).jdbc(jdbcUrl,"sino_person", prop)

    spark.stop()
  }

  def esRDD: Unit = {
    //配置spark与es连接
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ES2Mysql")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "192.168.211.144")
      .set("es.port", "9200")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //从es中index为test中查找内容为1的所有数据
    val esRDD: RDD[(String, collection.Map[String, AnyRef])] = EsSpark.esRDD(spark.sparkContext, "sino_person", "{\n  \"query\": {\n    \"match_all\": {}\n  }\n}")

    esRDD.map(_._2).foreach(x => {
      println(x)
      x.keySet.foreach(key => {
        val value: AnyRef = x(key)
        println(s"field:$key,value:$value")
      })
    })
    spark.stop()
  }
}
