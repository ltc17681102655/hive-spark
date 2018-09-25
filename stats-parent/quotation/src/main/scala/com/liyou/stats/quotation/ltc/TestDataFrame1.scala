package com.liyou.stats.quotation.ltc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Auther: ltc
 * @Date: 2018/9/21 13:14
 * @Description:
  *
  * +------+---+
  * |  name|age|
  * +------+---+
  * | spark| 10|
  * |hadoop| 20|
  * +------+---+
 */
object TestDataFrame1 {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("TestDataFrame1").setMaster("local")
    val conf = new SparkConf().setAppName("TestDataFrame1")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    val fileRDD = sc.textFile("E:\\666\\people.txt")

    val fileRDD = sc.textFile("hdfs://mycluster/user/liutongchun/spark.txt")
    // 将 RDD 数据映射成 Row，需要 import org.apache.spark.sql.Row
    val rowRDD: RDD[Row] = fileRDD.map(line => {
      val fields = line.split(",")
      Row(fields(0), fields(1).trim.toInt)
    })
    // 创建 StructType 来定义结构
    val structType: StructType = StructType(
      //字段名，字段类型，是否可以为空
      StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) :: Nil
    )
    /**
      * rows: java.util.List[Row],
      * schema: StructType
      **/
    val df: DataFrame = sqlContext.createDataFrame(rowRDD, structType)
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()
  }
}
