package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C4_GroupTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("com.spark.core.C4_GroupTop3").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sparkContext.textFile("./data/core/score.txt")
    val map: RDD[(String, Int)] = rdd.map(line => (line.split(" ")(0),line.split(" ")(1).toInt))
    val groupByKey: RDD[(String, Iterable[Int])] = map.groupByKey()
    val groupTop3: RDD[(String, List[Int])] = groupByKey.map(element => {
      val key: String = element._1
      val ints: Iterable[Int] = element._2
      val sorted: List[Int] = ints.toList.sorted  //对List里的int值进行排序(升序)
      val reverse: List[Int] = sorted.reverse    //将排序的值进行反转(降序)
      val take: List[Int] = reverse.take(3)     //取出排名前3的
      (key, take)
    })
    groupTop3.foreach(println(_))
  }
}
