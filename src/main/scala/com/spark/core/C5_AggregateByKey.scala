package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求class1与class2的总分数
  * aggregateByKey : 类似于map-reduce
  */
object C5_AggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("com.spark.core.C4_AggregateByKey").setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    val lines = sparkContext.textFile("./data/core/score.txt")
    val pairs: RDD[(String, Int)] = lines.map(line=>(line.split(" ")(0),line.split(" ")(1).toInt))

    // aggregateByKey，分为三个参数
    // reduceByKey认为是aggregateByKey的简化版
    // aggregateByKey最重要的一点是，多提供了一个函数，Seq Function
    // 就是说自己可以控制如何对每个partition中的数据进行先聚合，类似于mapreduce中的，map-side combine
    // 然后才是对所有partition中的数据进行全局聚合

    // 第一个参数是，每个key的初始值
    // 第二个是个函数，Seq Function，如何进行shuffle map-side的本地聚合
    // 第三个是个函数，Combiner Function，如何进行shuffle reduce-side的全局聚合
    val wordCounts = pairs.aggregateByKey(0)((v1: Int, v2: Int) => {
      //println("v1="+v1+",v2="+v2)
      (v1 + v2)
    }, (v1: Int, v2: Int) => {
      println("v1="+v1+",v2="+v2)
      (v1 + v2)
    })

    wordCounts.collect().foreach(println)
  }
}
