package com.exm.spark.impor.data

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

object MySQL2ES {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MySQL2ES").setMaster("local[1]")
    sparkConf.set("es.nodes", "192.168.211.144")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.auto.create", "true")  //自动创建ES
    sparkConf.set("es.write.operation", "index")
    //es.write.operation : 写入模式
    //https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val url: String = "jdbc:mysql://192.168.211.144:3306/sino"
    val table: String = "person"
    val properties: Properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    properties.put("driver", "com.mysql.jdbc.Driver")
    val df: DataFrame = sparkSession.read.jdbc(url, table, properties)
    df.show()
    EsSparkSQL.saveToEs(df, "sino_person/sino_person")
    sparkSession.stop()
  }

}
