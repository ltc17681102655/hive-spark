package com.liyou.stats.quotation.ltc

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Auther: ltc
 * @Date: 2018/9/21 13:34
 * @Description:
  * +---+----+------+----+----+
  * | id|name|status|sort|flag|
  * +---+----+------+----+----+
  * |  1|热门城市|     1|  10|   3|
  * |  2| 长三角|     1|  20|   0|
  * |  3| 珠三角|     1|  30|   0|
  * |  4| 京津冀|     1|  40|   0|
  * |  5|  其他|     1|  50|   0|
  * |  6|  美国|     1|  60|   0|
  * |  7|  澳洲|     1|  70|   0|
  * |  8|  日本|     1|  80|   0|
  * +---+----+------+----+----+
 */
object TestMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestMysql")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val url = "jdbc:mysql://mysql.test.tuboshi.co:3306/sHouseApp_pre"
    val table = "city_category"
    val properties = new Properties()
    properties.setProperty("user", "tbs")
    properties.setProperty("password", "LangBoshi^666")
    properties.setProperty("driver","com.mysql.jdbc.Driver")
    //需要传入Mysql的URL、表明、properties（连接数据库的用户名密码）
    val df = sqlContext.read.jdbc(url, table, properties)
    df.createOrReplaceTempView("city_category")
    sqlContext.sql("select * from city_category").show()

  }
}
