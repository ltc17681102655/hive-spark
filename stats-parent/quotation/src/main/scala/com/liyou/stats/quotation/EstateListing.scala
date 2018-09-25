package com.liyou.stats.quotation

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * <pre>
  *
  * @description
  * @copyright: Copyright (c)2017
  * @author: vincent
  * @version: 1.0
  * @date: 2018/9/8
  *        </pre>
  */
object EstateListing {
  private val LOGGER = LoggerFactory.getLogger("EstateListing")

  private val TAG_INDEX_CITY: Int = 0
  private val TAG_INDEX_DISTRICT: Int = 1
  private val TAG_INDEX_PLATE: Int = 2
  private val TAG_INDEX_HOUSE: Int = 3
  private val TAG_INDEX_FIRST_PUB_DATE = 4
  private val TAG_INDEX_PUB_DATE: Int = 5
  private val TAG_INDEX_AREA: Int = 6
  private val TAG_INDEX_QUOTED_PRICE: Int = 7

  private val TAG_RANGE_START: String = "s"
  private val TAG_RANGE_END: String = "e"


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("头部价-挂牌")
      .master("local[*]")
      .config("queue", "queue_cem")
      .enableHiveSupport()
      .getOrCreate()

    val ds: String = App.getLatestDay
    val upDate: String = App.getToday

    val sql = "SELECT " +
      "a.city_id,b.biz_district_id AS district_id,split(b.plate_fk,'@')[1] AS plate_id,a.house_id," +
      "a.first_pub_date," +
      "case when a.last_pub_date IS NULL THEN  a.first_pub_date  WHEN a.last_pub_date >= '" + upDate + "' THEN '" + ds + "' ELSE a.last_pub_date END AS pub_date" +
      ",a.area,a.quoted_price " +
      "FROM liyou_test_db.stg_estate_pub_info a JOIN liyou_test_db.dim_house_community b ON a.city_id = b.biz_city_id AND a.house_id = b.biz_house_id " +
      "WHERE a.ds='" + ds + "' AND a.status IN ('3','5','7') " +
      "AND a.total_floors < 1000 AND a.area >0 AND a.area <1000 " +
      "AND a.quoted_price/a.area BETWEEN 100000 and 50000000 " +
      "and a.evaluation_high_price/a.quoted_price <=2  " +
      "AND a.first_pub_date < '" + upDate + "'"

    val rdd = spark.sql(sql).rdd

    val cityRdd = rdd
      .groupBy(c => c.getString(TAG_INDEX_CITY))
      .flatMap(dim_city => statsCity(dim_city._2, dim_city._1))

    val tempView = "dws_stats_quotation_listing"
    spark.createDataFrame(cityRdd, App.SCHEMA).createOrReplaceTempView(tempView)
//    spark.sql("insert into liyou_test_db.dws_stats_quotation partition(ds='" + ds + "') from " + tempView)
    spark.sql("insert into liutongchun.dws_stats_quotation partition(ds='" + ds + "') from " + tempView)
  }

  def statsCity(rdd: Iterable[Row], cityId: String): Array[Row] = {
    val dim: String = "SECOND"

    var list = stats(rdd, dim, App.TAG_OBJ_TYPE_CITY, App.markCityObject(cityId), false)

    // 区域
    val district = rdd.groupBy(c => c.getString(TAG_INDEX_DISTRICT))
      .flatMap(district => stats(district._2, dim, App.TAG_OBJ_TYPE_DISTRICT, App.markDistrictObject(cityId, district._1), false))
      .toArray
    list = list ++ district

    // 板块
    val plate = rdd.groupBy(c => c.getString(TAG_INDEX_PLATE))
      .flatMap(plate => stats(plate._2, dim, App.TAG_OBJ_TYPE_PLATE, App.markPlateObject(cityId, plate._1), false))
      .toArray
    list ++ plate

    // 板块
    val house = rdd.groupBy(c => c.getString(TAG_INDEX_HOUSE))
      .flatMap(house => stats(house._2, dim, App.TAG_OBJ_TYPE_HOUSE, App.markHouseObject(cityId, house._1), true))
      .toArray
    list ++ house
  }

  /**
    * 非小区统计
    *
    * @param rdd
    * @param dim
    * @param objValue
    * @param objType
    * @return
    */
  def stats(rdd: Iterable[Row], dim: String, objType: String, objValue: String, isHouse: Boolean): Array[Row] = {
    var list = List[Row]()
    if (rdd == null || rdd.isEmpty) {
      return list.toArray
    }

    val latestDate = rdd.map(c => c.getString(TAG_INDEX_PUB_DATE)).max.split(" ")(0)
    val periodMap = App.getPeriod(latestDate)
    if (periodMap == null) {
      LOGGER.info("获取分区时段失败，objType：{},objValue：{},latestDate:{}", objType, objValue, latestDate)
      return list.toArray
    }

    val thisPeriod = rdd.filter(
      c => {
        val firstPubDate = c.getString(TAG_INDEX_FIRST_PUB_DATE).split(" ")(0)
        val lastPubDate = c.getString(TAG_INDEX_PUB_DATE).split(" ")(0)

        firstPubDate <= periodMap.getOrElse(App.TAG_PERIOD_KEY_PERIOD_END, "") && lastPubDate >= periodMap.getOrElse(App.TAG_PERIOD_KEY_PERIOD_START, "")
      })

    val previousPeriod = rdd.filter(
      c => {
        val firstPubDate = c.getString(TAG_INDEX_FIRST_PUB_DATE).split(" ")(0)
        val lastPubDate = c.getString(TAG_INDEX_PUB_DATE).split(" ")(0)

        firstPubDate <= periodMap.getOrElse(App.TAG_PERIOD_KEY_PRE_PERIOD_START, "") && lastPubDate >= periodMap.getOrElse(App.TAG_PERIOD_KEY_PRE_PERIOD_END, "")
      })

    val thisVolume = thisPeriod.size.toDouble
    val thisAveragePrice = if (isHouse) getAveragePriceOfHouse(thisPeriod) else getAveragePrice(thisPeriod)
    val periodValue = periodMap.getOrElse(App.TAG_PERIOD_KEY_PERIOD, "")
    if (previousPeriod.nonEmpty) {
      // 挂牌均价
      val previousAveragePrice = if (isHouse) getAveragePriceOfHouse(previousPeriod) else getAveragePrice(previousPeriod)
      val monAveragePriceRate = (thisAveragePrice - previousAveragePrice) * 100 / previousAveragePrice
      list = Row(dim, App.TAG_INDEX_QUOTED_PRICE_AVG, objType, objValue, App.TAG_PERIOD_TYPE_PERIOD, periodValue, thisAveragePrice, previousAveragePrice, monAveragePriceRate, null) :: list
      // 挂牌量
      val previousVolume = previousPeriod.size.toDouble
      val monVolumeRate = (thisVolume - previousVolume) * 100 / previousVolume
      list = Row(dim, App.TAG_INDEX_QUOTED_VOLUME, objType, objValue, App.TAG_PERIOD_TYPE_PERIOD, periodValue, thisVolume, previousVolume, monVolumeRate, null) :: list
    } else {
      // 挂牌均价
      list = Row(dim, App.TAG_INDEX_QUOTED_PRICE_AVG, objType, objValue, App.TAG_PERIOD_TYPE_PERIOD, periodValue, thisAveragePrice, null, null, null) :: list
      // 挂牌量
      list = Row(dim, App.TAG_INDEX_QUOTED_VOLUME, objType, objValue, App.TAG_PERIOD_TYPE_PERIOD, periodValue, thisVolume, null, null, null) :: list
    }

    list.toArray
  }

  def getAveragePrice(rdd: Iterable[Row]): Double = {
    rdd.map(c => c.getString(TAG_INDEX_QUOTED_PRICE).toDouble / 100).sum / rdd.map(c => c.getString(TAG_INDEX_AREA).toDouble).sum
  }

  /**
    * 小区挂牌均价统计
    * <li>
    * 先计算x天内主面积段报价数量
    * 如主面积段报价数量不足以覆盖50%以上的报价， 则取主面积段和次面积段，依次类推，直至取的面积段覆盖50%以上报价为止
    * 然后在所取面积段中计算众数均价，没有众数时取面积段的报价均价
    * <li>
    *
    * <li>
    * 如果选取的面积段众数均价与不分面积段均价相差20%以上，则取不分面积段均价
    * <li>
    *
    * @param rdd
    * @return
    */
  def getAveragePriceOfHouse(rdd: Iterable[Row], segment: Int = 3): Double = {
    val volume = rdd.size
    if (volume <= segment) {
      return getAveragePrice(rdd)
    }

    // 取主力面积段
    val main = rdd.map(c => (getAreaRange(c.getString(TAG_INDEX_AREA).toInt), c))
      .groupBy(_._1).map(c => (c._1, c._2.size, c._2))
      .toSeq.sortBy(_._2)
      .reduceLeft((x, y) => if ((x._2 / volume) > 0.5) x else (x._1, x._2 + y._2, x._3 ++ y._3))._3
      .map(_._2)

    // 求均价众数
    val averages = main.map(c => (c.getString(TAG_INDEX_QUOTED_PRICE).toLong / 100) / c.getString(TAG_INDEX_AREA).toLong)
    val crowd = getModalNumber(averages, segment)
    val avg = getAveragePrice(main)

    if (crowd.isNaN || Math.abs((crowd - avg) / crowd) > 0.2) {
      avg
    } else {
      crowd
    }
  }

  val areaRange = List(
    Map(TAG_RANGE_START -> 0, TAG_RANGE_END -> 60),
    Map(TAG_RANGE_START -> 60, TAG_RANGE_END -> 70),
    Map(TAG_RANGE_START -> 70, TAG_RANGE_END -> 80),
    Map(TAG_RANGE_START -> 80, TAG_RANGE_END -> 90),
    Map(TAG_RANGE_START -> 90, TAG_RANGE_END -> 100),
    Map(TAG_RANGE_START -> 100, TAG_RANGE_END -> 110),
    Map(TAG_RANGE_START -> 110, TAG_RANGE_END -> 120),
    Map(TAG_RANGE_START -> 120, TAG_RANGE_END -> 135),
    Map(TAG_RANGE_START -> 135, TAG_RANGE_END -> 150),
    Map(TAG_RANGE_START -> 150, TAG_RANGE_END -> 165),
    Map(TAG_RANGE_START -> 165, TAG_RANGE_END -> 180),
    Map(TAG_RANGE_START -> 180, TAG_RANGE_END -> 200),
    Map(TAG_RANGE_START -> 200, TAG_RANGE_END -> 225),
    Map(TAG_RANGE_START -> 225, TAG_RANGE_END -> 250),
    Map(TAG_RANGE_START -> 250, TAG_RANGE_END -> 275),
    Map(TAG_RANGE_START -> 275, TAG_RANGE_END -> 300),
    Map(TAG_RANGE_START -> 300, TAG_RANGE_END -> 350),
    Map(TAG_RANGE_START -> 350, TAG_RANGE_END -> 400),
    Map(TAG_RANGE_START -> 400, TAG_RANGE_END -> 450),
    Map(TAG_RANGE_START -> 450, TAG_RANGE_END -> 500),
    Map(TAG_RANGE_START -> 500, TAG_RANGE_END -> 999999)
  )

  def getAreaRange(area: Int): Map[String, Int] = {
    val filterRange = areaRange.filter(c => area > c.getOrElse(TAG_RANGE_START, 0) && area <= c.getOrElse(TAG_RANGE_END, 0))
    if (filterRange.nonEmpty) filterRange.head else Map(TAG_RANGE_START -> 90, TAG_RANGE_END -> 100)
  }

  def getModalNumber(averages: Iterable[Long], segment: Int): Double = {
    val min = averages.min
    val max = averages.max
    val pitch = (max - min) / segment

    val allocation = averages
      .map(c => (Math.ceil((c - min).toDouble / pitch).toInt, c))
      .groupBy(_._1)
      .map(c => (c._1, c._2.size, c._2))
      .toSeq.sortBy(_._2).reverse

    if (allocation.isEmpty) {
      return Double.NaN
    }

    if (allocation.size == 1) {
      return allocation.head._3.map(_._2).sum / allocation.head._2
    }

    if (allocation.head._2 > allocation(1)._2) {
      allocation.head._3.map(_._2).sum / allocation.head._2
    } else {
      Double.NaN
    }
  }
}
