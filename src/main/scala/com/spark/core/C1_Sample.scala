package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object C1_Sample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("com.spark.core.C1_Sample").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val list = List("张三", "李四", "王五", "赵六", "张三_1", "李四_1", "王五_1", "赵六_1", "王五_2", "赵六_2")
    val rdd = sparkContext.parallelize(list)
    rdd.foreach(x=>println(x))
    sparkContext.stop
  }
}
