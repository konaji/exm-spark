package com.exm.spark.impor.data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Hive2MySQL {

  case class Blog(name: String, count: Int)

  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Hive2MySQL")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext
    val data = sparkContext.makeRDD(List(("www", 10), ("iteblog", 20), ("com", 30)))

    import spark.implicits._

    val df = data.map(x => new Blog(x._1, x._2)).toDF()
//    df.write.mode(SaveMode.Append).format("jdbc")
//      .option("url", "jdbc:mysql://192.168.211.144:3306/sino")
//      .option("dbtable", "person")
//      .option("user", "root")
//      .option("password", "root")
//      .save()

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
//    spark.sql("select * from dataBlock")
//      .write.mode("append")
//      .saveAsTable("hadoop10.data_block")


    //创建hive表
    spark.sql("create table person (id int,name String) row format delimited fields terminated by ','")
    //加载表数据
    spark.sql("load data local inpath './data/person.txt' into table person")
    //查询
    spark.sql("select * from person").show()
    //查询hive表的非默认库时，需要用库名.表名访问数据
    //spark.sql("select * from itcast.t1").show()

    sparkContext.stop
  }


}
