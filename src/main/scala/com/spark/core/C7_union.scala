package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将两个RDD原封不动的合并, 不会去重!
  */
object C7_Union {
  def main(args: Array[String]): Unit = {
    //testString
    testInt
  }

  def testString: Unit = {
    val conf: SparkConf = new SparkConf().setAppName("com.spark.core.C7_union").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val list1 = List("张三","李四","王五")
    val list2 = List("王五","赵六")
    val rdd1: RDD[String] = sparkContext.parallelize(list1)
    val rdd2: RDD[String] = sparkContext.parallelize(list2)
    val union: RDD[String] = rdd1.union(rdd2) //将rdd1与rdd2的内容合并(不会去重,合并后有两个王五)
    union.collect().foreach(println(_))
  }

  def testInt: Unit = {
    val conf: SparkConf = new SparkConf().setAppName("com.spark.core.C7_union").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val list1 = List(1,2)
    val list2 = List(2,3)
    val rdd1: RDD[Int] = sparkContext.parallelize(list1)
    val rdd2: RDD[Int] = sparkContext.parallelize(list2)
    val union: RDD[Int] = rdd1.union(rdd2) //将rdd1与rdd2的内容合并(不会去重,合并后有两个2)
    union.collect().foreach(println(_))
  }
}
