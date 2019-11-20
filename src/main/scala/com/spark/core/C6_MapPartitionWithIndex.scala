package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C6_MapPartitionWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("com.spark.core.C6_MapPartitionWithIndex").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sparkContext.parallelize(List("张三", "李四", "王五", "赵六"), 2)
    /**
      * mapPartitionsWithIndex : 实现mapPartitions的同时,遍历每个元素时还可以获取到分区ID
      */
    rdd.mapPartitionsWithIndex((index, element) => {
      println(index)//当前分区ID
      element.toList.iterator
    }, true).collect().foreach(println(_))
  }
}
