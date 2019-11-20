package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C3_Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("com.spark.core.C3_Top3").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sparkContext.textFile("./data/core/top.txt")
    val map: RDD[(Int, String)] = rdd.map(x=>(x.toInt,x))
    val sortByKey: RDD[(Int, String)] = map.sortByKey(false)//true:升序,false:降序
    sortByKey.take(3).foreach(x=>println(x._2))//take(3):取出前三个元素
    sparkContext.stop
  }
}
