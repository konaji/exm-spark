package com.exm.spark.impor.data

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * 用SparkSQL创建Hive表,并加载数据
  */
object File2Hive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("File2Hive")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val sparkContext: SparkContext = spark.sparkContext

    /**
      * 在本地电脑上运行可能会报错 :
      * Got exception: org.apache.hadoop.security.AccessControlException Permission denied:
      *     user=¼, access=WRITE, inode="/user/hive/warehouse":root:supergroup:drwxr-xr-x
      * 这是因为在HDFS创建文件的时候,没有权限,在服务器上运行是没问题的
      * 或者修改HDFS /user里面的权限
      */

    //创建hive表
    spark.sql("create table person (id int,name String) row format delimited fields terminated by ','")
    //加载表数据
    spark.sql("load data local inpath './data/person.txt' into table person")
    //查询
    spark.sql("select * from person").show()
    //查询hive表的非默认库时，需要用库名.表名访问数据
    //spark.sql("select * from databaseName.table").show()

    sparkContext.stop
  }
}
