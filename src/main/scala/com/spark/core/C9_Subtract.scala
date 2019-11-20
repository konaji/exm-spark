package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object C9_Subtract {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("com.spark.core.C9_Subtract").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)

    val list1 = List("张三","张三","李四","王五")
    val list2 = List("王五","王五","赵六")
    val rdd1: RDD[String] = sparkContext.parallelize(list1)
    val rdd2: RDD[String] = sparkContext.parallelize(list2)

    //取在rdd1中有并且在rdd2中没有的元素(不去重!!)
    val intersection: RDD[String] = rdd1.subtract(rdd2)
    intersection.collect().foreach(println(_))//这里取到的是"张三","张三","李四"

  }
}
