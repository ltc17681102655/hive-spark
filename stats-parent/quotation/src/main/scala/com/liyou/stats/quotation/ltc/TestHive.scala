package com.liyou.stats.quotation.ltc

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object TestHive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("select * from liutongchun.stg_city_category").show()
  }
}
