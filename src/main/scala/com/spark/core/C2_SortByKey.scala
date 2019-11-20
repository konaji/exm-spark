package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C2_SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("com.spark.core.C2_SortByKey").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val rdd = sparkContext.textFile("./data/core/sort.txt")
    val map: RDD[String] = rdd.map(line => {
      println("line=" + line)
      line
    })
    map.foreach(println(_))
    println("=======================================")
    val pairs: RDD[(SecondSortKey, String)] = rdd.map { line =>
      (new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line)
    }
    val sortByKey = pairs.sortByKey()
    sortByKey.foreach(x => println(x._2))
    sparkContext.stop
  }
}

class SecondSortKey(val first: Int, val second: Int) extends Ordered[SecondSortKey] with Serializable {
  override def compare(that: SecondSortKey): Int = {
    if (this.first - that.first != 0) {
      this.first - that.first
    } else {
      this.second - that.second
    }
  }
}