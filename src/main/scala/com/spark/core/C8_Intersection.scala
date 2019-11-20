package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C8_Intersection {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("com.spark.core.C8_Intersection").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val list1 = List("张三","李四","王五","王五")
    val list2 = List("王五","王五","赵六")
    val rdd1: RDD[String] = sparkContext.parallelize(list1)
    val rdd2: RDD[String] = sparkContext.parallelize(list2)

    // 获取两个RDD的交集,并且去重
    val intersection: RDD[String] = rdd1.intersection(rdd2)
    intersection.collect().foreach(println(_)) // 这里只有一个"王五"被打印

  }
}
