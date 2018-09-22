package com.liyou.stats.quotation


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.liyou.stats.quotation.App.Period
import com.liyou.stats.quotation.App.Period.Period
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory

/**
  * <pre>
  *
  * @description
  * @copyright: Copyright (c)2017
  * @author: vincent
  * @version: 1.0
  * @date: 2018/9/7
  *        </pre>
  */
object EstateDeal {
  private val LOGGER = LoggerFactory.getLogger("EstateDeal")

  private val TAG_INDEX_CITY: Int = 0
  private val TAG_INDEX_DISTRICT: Int = 1
  private val TAG_INDEX_PLATE: Int = 2
  private val TAG_INDEX_HOUSE: Int = 3
  private val TAG_INDEX_BARGAIN_DATE: Int = 4
  private val TAG_INDEX_TOTAL_PRICE: Int = 5
  private val TAG_INDEX_BARGAIN_AREA: Int = 6
  private val TAG_INDEX_AVERAGE_PRICE: Int = 7

  private val ds = App.getLatestDay

  val firstDealSql = "SELECT " +
    "a.city_id,b.biz_district_id AS district_id,split(b.plate_fk,'@')[1] AS plate_id,a.house_id," +
    "a.bargaindate,a.totalprice,a.bargain_area,a.sinprice " +
    "FROM liyou_test_db.stg_house_detail_deal_data a JOIN liyou_test_db.dim_house_community b ON a.city_id = b.biz_city_id AND a.house_id = b.biz_house_id " +
    "WHERE a.ds='" + ds + "' and b.is_first = 1"

  val secondDealSql = "SELECT " +
    "a.city_id,b.biz_district_id AS district_id,split(b.plate_fk,'@')[1] AS plate_id,a.house_id," +
    "a.bargaindate," +
    "CASE WHEN a.price_label in('1','签约实价') THEN a.totalprice WHEN a.price_label in('2','行情参考') THEN a.eval_totalprice  ELSE a.totalprice END as totalprice," +
    "a.bargain_area,a.sinprice " +
    "FROM liyou_test_db.stg_house_detail_deal_data a JOIN liyou_test_db.dim_house_community b ON a.city_id = b.biz_city_id AND a.house_id = b.biz_house_id " +
    "WHERE a.ds='" + ds + "' and b.is_first = 2"

  var cityUpdateMap: Map[String, String] = Map()
  var dealStats: DealStats = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("头部价-成交")
      .master("local[*]")
      .config("queue", "queue_cem")
      .enableHiveSupport()
      .getOrCreate()

    fillCityUpdateMap(spark)

    dealStats = DealStats(cityUpdateMap)

    val newRdd = spark.sql(firstDealSql).rdd
      .groupBy(c => c.getString(TAG_INDEX_CITY))
      .flatMap(dim_city => statsNewCity(dim_city._2, dim_city._1))

    val secondRdd = spark.sql(secondDealSql).rdd
      .groupBy(c => c.getString(TAG_INDEX_CITY))
      .flatMap(dim_city => statsSecondCity(dim_city._2, dim_city._1))

    val tempView = "dws_stats_quotation_deal"
    spark.createDataFrame(newRdd.union(secondRdd), App.SCHEMA).createOrReplaceTempView(tempView)
//    spark.sql("insert into liyou_test_db.dws_stats_quotation partition(ds='" + ds + "') from " + tempView)
    spark.sql("insert into liutongchun.dws_stats_quotation partition(ds='" + ds + "') from " + tempView)
  }

  def fillCityUpdateMap(spark: SparkSession) = {
    val data = spark.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://mysqlslave.tuboshi.co:3306/sHouseApp_pre?useCursorFetch=true&useSSL=false&zeroDateTimeBehavior=convertToNull",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "city_data_updated_record",
        "user" -> "bi",
        "password" -> "bi@Liyou123")).load()

    val format = new SimpleDateFormat("yyyy-MM-dd")

    data.foreach(c => {
      if (c.getDate(1) != null) {
        cityUpdateMap += (c.getLong(0) + "_1" -> format.format(new Date(c.getDate(1).getTime)))
      }

      if (c.getDate(2) != null) {
        cityUpdateMap += (c.getLong(0) + "_2" -> format.format(new Date(c.getDate(2).getTime)))
      }
    })
  }

  /**
    * 新房城市维度统计
    *
    * @param rdd
    * @param obj
    * @return
    */
  def statsNewCity(rdd: Iterable[Row], obj: String): Array[Row] = {
    val dim = App.TAG_DIM_FIRST

    var list = List[Row]()
    list = list ++ dealStats.getStats(rdd, dim, App.TAG_OBJ_TYPE_CITY, App.markCityObject(obj), (Period.Cyclical, Period.Cyclical))

    // 区域
    val district = rdd.groupBy(c => c.getString(TAG_INDEX_DISTRICT))
      .flatMap(district => dealStats.getStats(district._2, dim, App.TAG_OBJ_TYPE_DISTRICT, App.markDistrictObject(obj, district._1), (Period.Cyclical, Period.Cyclical)))
      .toArray
    list = list ++ district

    // 板块
    val plate = rdd.groupBy(c => c.getString(TAG_INDEX_PLATE))
      .flatMap(plate => dealStats.getStats(plate._2, dim, App.TAG_OBJ_TYPE_PLATE, App.markPlateObject(obj, plate._1), (Period.Cyclical, Period.Cyclical)))
      .toArray
    list = list ++ plate

    // 小区
    val house = rdd.groupBy(c => c.getString(TAG_INDEX_HOUSE))
      .flatMap(district => dealStats.getStats(district._2, dim, App.TAG_OBJ_TYPE_HOUSE, App.markHouseObject(obj, district._1), (Period.Cyclical, Period.Cyclical)))
      .toArray
    list = list ++ house

    list.toArray
  }

  def statsSecondCity(rdd: Iterable[Row], obj: String): Array[Row] = {
    val dim = App.TAG_DIM_SECOND
    var list = List[Row]()
    list = list ++ dealStats.getStats(rdd, dim, App.TAG_OBJ_TYPE_CITY, App.markCityObject(obj), (Period.Cyclical, Period.EjectMonth))

    // 区域
    val district = rdd.groupBy(c => c.getString(TAG_INDEX_DISTRICT))
      .flatMap(district => dealStats.getStats(district._2, dim, App.TAG_OBJ_TYPE_DISTRICT, App.markDistrictObject(obj, district._1), (Period.Cyclical, Period.EjectMonth)))
      .toArray
    list = list ++ district

    // 板块
    val plate = rdd.groupBy(c => c.getString(TAG_INDEX_PLATE))
      .flatMap(plate => dealStats.getStats(plate._2, dim, App.TAG_OBJ_TYPE_PLATE, App.markPlateObject(obj, plate._1), (Period.Cyclical, Period.EjectMonth)))
      .toArray
    list = list ++ plate

    // 小区
    val house = rdd.groupBy(c => c.getString(TAG_INDEX_HOUSE))
      .flatMap(house => dealStats.getStats(house._2, dim, App.TAG_OBJ_TYPE_HOUSE, App.markHouseObject(obj, house._1), (Period.Cyclical, Period.DealMonth)))
      .toArray

    list = list ++ house

    list.toArray
  }

  object DealStats {
    def apply(cityUpdateMap: Map[String, String]): DealStats = new DealStats(cityUpdateMap)
  }

  /**
    *
    * @param cityUpdateMap
    */
  class DealStats(cityUpdateMap: Map[String, String]) {
    def getStats(rdd: Iterable[Row], dim: String, objType: String, objValue: String, tuple: (Period, Period)): Array[Row] = {
      var list = List[Row]()
      if (rdd == null || rdd.isEmpty) {
        return list.toArray
      }

      // 成交总量
      list = getDealTotalVolume(rdd, dim, objType, objValue) :: list

      // 成交均价
      val priceTuple = getPeriodMap(rdd, tuple._1, dim)
      if (priceTuple != null && priceTuple._2 != null && priceTuple._2._2 != null && priceTuple._2._2.nonEmpty) {
        list = getDealPrice(dim, objType, objValue, priceTuple._1, priceTuple._2) :: list
      }

      // 成交量
      val volumeTuple = if (tuple._1 == tuple._2) priceTuple else getPeriodMap(rdd, tuple._2, dim)
      if (volumeTuple != null && volumeTuple._2 != null && volumeTuple._2._2 != null && volumeTuple._2._2.nonEmpty) {
        list = getDealVolume(dim, objType, objValue, volumeTuple._1, volumeTuple._2) :: list
      }

      list.toArray
    }

    private def getDealTotalVolume(rdd: Iterable[Row], dim: String, objType: String, objValue: String): Row = {
      val count: Double = if (rdd == null) 0 else rdd.size.toDouble

      Row(dim, App.TAG_INDEX_DEAL_TOTAL_VOLUME, objType, objValue, App.TAG_PERIOD_TYPE_TOTAL, App.TAG_PERIOD_TYPE_TOTAL, count, null, null, null)
    }

    /**
      * 计算成交均价
      *
      * @param dim
      * @param objValue
      * @param objType
      * @param periodType
      * @param tuple
      * @return
      */
    private def getDealPrice(dim: String, objType: String, objValue: String, periodType: String, tuple: (String, Iterable[Row], Iterable[Row])): Row = {
      if (tuple == null || tuple._2 == null || tuple._2.isEmpty) {
        return null
      }

      val period: String = tuple._1
      val thisAveragePrice = getAveragePrice(tuple._2)
      if (tuple._3 == null || tuple._3.nonEmpty) {
        // 成交均价
        val previousAveragePrice = getAveragePrice(tuple._3)
        val monAveragePriceRate = (thisAveragePrice - previousAveragePrice) * 100 / previousAveragePrice
        Row(dim, App.TAG_INDEX_DEAL_PRICE_AVG, objType, objValue, periodType, period, thisAveragePrice, previousAveragePrice, monAveragePriceRate, null)
      } else {
        Row(dim, App.TAG_INDEX_DEAL_PRICE_AVG, objType, objValue, periodType, period, thisAveragePrice, null, null, null)
      }
    }

    /**
      * 计算成交量
      *
      * @param dim
      * @param objValue
      * @param objType
      * @param periodType
      * @param tuple
      * @return
      */
    private def getDealVolume(dim: String, objType: String, objValue: String, periodType: String, tuple: (String, Iterable[Row], Iterable[Row])): Row = {
      if (tuple == null || tuple._2 == null || tuple._2.isEmpty) {
        return null
      }

      val period: String = tuple._1

      val thisVolume = tuple._2.size.toDouble
      val previousPeriod = tuple._3
      if (previousPeriod.nonEmpty) {
        // 成交量
        val previousVolume = previousPeriod.size.toDouble
        val monVolumeRate = (thisVolume - previousVolume) * 100 / previousVolume
        Row(dim, App.TAG_INDEX_DEAL_VOLUME, objType, objValue, periodType, period, thisVolume, previousVolume, monVolumeRate, null)
      } else {
        Row(dim, App.TAG_INDEX_DEAL_VOLUME, objType, objValue, periodType, period, thisVolume, null, null, null)
      }
    }

    private def getPeriodMap(rdd: Iterable[Row], period: Period, dim: String): (String, (String, Iterable[Row], Iterable[Row])) = {
      period match {
        case Period.DealMonth => (App.TAG_PERIOD_TYPE_MONTH, getMonthAndData(rdd))
        case Period.EjectMonth => (App.TAG_PERIOD_TYPE_MONTH, getOutMonthAndData(rdd, if (dim.eq(App.TAG_DIM_FIRST)) App.TAG_FIRST else App.TAG_SECOND))
        case Period.Cyclical => (App.TAG_PERIOD_TYPE_PERIOD, getPeriodAndData(rdd))
      }
    }

    /**
      * 周期性数据统计
      *
      * @param rdd
      * @return
      */
    private def getPeriodAndData(rdd: Iterable[Row]): (String, Iterable[Row], Iterable[Row]) = {
      val latestBargainDate = rdd.map(_.getString(TAG_INDEX_BARGAIN_DATE)).max
      val map = App.getPeriod(latestBargainDate)
      if (map == null) {
        LOGGER.info("获取周期性数据失败:{}", latestBargainDate)
        return null
      }

      getDataRangMap(rdd, map)
    }

    /**
      *
      * 实际成交统计
      *
      * @param rdd
      * @return
      */
    private def getMonthAndData(rdd: Iterable[Row]): (String, Iterable[Row], Iterable[Row]) = {
      val lastBargainDate = rdd.map(_.getString(TAG_INDEX_BARGAIN_DATE)).max

      val map = App.getPeriodMonth(lastBargainDate)
      if (map == null) {
        LOGGER.info("获取成交MAP失败")
        return null
      }

      getDataRangMap(rdd, map)
    }

    /**
      * 出仓月数据统计
      *
      * @param rdd
      * @return
      */
    private def getOutMonthAndData(rdd: Iterable[Row], firstHand: String): (String, Iterable[Row], Iterable[Row]) = {
      val tag = rdd.head.getString(TAG_INDEX_CITY) + "_" + firstHand
      val option = cityUpdateMap.get(tag)
      if (option.isEmpty) {
        LOGGER.info("获取出仓月MAP失败:{}", tag)
        return null
      }
      val map = App.getPeriodMonth(option.get)
      if (map == null) {
        LOGGER.info("获取出仓月MAP失败")
        return null
      }

      getDataRangMap(rdd, map)
    }

    /**
      * 获取统计区间
      *
      * @param rdd
      * @param map
      * @return
      */
    private def getDataRangMap(rdd: Iterable[Row], map: Map[String, String]): (String, Iterable[Row], Iterable[Row]) = {
      val thisPeriod = rdd.filter(
        c => c.getString(TAG_INDEX_BARGAIN_DATE) <= map.getOrElse(App.TAG_PERIOD_KEY_PERIOD_END, "")
          && c.getString(TAG_INDEX_BARGAIN_DATE) >= map.getOrElse(App.TAG_PERIOD_KEY_PERIOD_START, ""))

      val previousPeriod = rdd.filter(
        c => c.getString(TAG_INDEX_BARGAIN_DATE) <= map.getOrElse(App.TAG_PERIOD_KEY_PRE_PERIOD_END, "") &&
          c.getString(TAG_INDEX_BARGAIN_DATE) >= map.getOrElse(App.TAG_PERIOD_KEY_PRE_PERIOD_START, ""))

      (map.getOrElse(App.TAG_PERIOD_KEY_PERIOD, ""), thisPeriod, previousPeriod)
    }

    def getAveragePrice(rdd: Iterable[Row]): Double = {
      rdd.map(c => c.getString(TAG_INDEX_TOTAL_PRICE).toDouble).sum / rdd.map(c => c.getString(TAG_INDEX_BARGAIN_AREA).toDouble).sum
    }
  }

}
