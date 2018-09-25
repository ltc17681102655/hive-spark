package com.liyou.stats.quotation.ltc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: ltc
  * @Date: 2018/9/20 22:28
  * @Description:
  *
  * 结果
  * (scala,1)
  * (hello,1)
  * (java,1)
  * (hadoop,2)
  * (demo,1)
  */
object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(args(0))

    line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)

    sc.stop()
  }
}
