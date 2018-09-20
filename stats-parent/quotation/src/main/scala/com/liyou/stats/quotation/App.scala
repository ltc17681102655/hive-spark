package com.liyou.stats.quotation

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Calendar, Date}

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * <pre>
  *
  * @description
  * @copyright: Copyright (c)2017
  * @author: vincent
  * @version: 1.0
  * @date: 2018/9/10
  *        </pre>
  */
object App {

  /**
    * dws_stats_header 表结构
    */
  val SCHEMA = StructType(List(
    StructField("dim", StringType, false),
    StructField("index", StringType, false),
    StructField("obj_type", StringType, false),
    StructField("obj_value", StringType, false),
    StructField("period_type", StringType, false),
    StructField("period_value", StringType, false),
    StructField("current_result", DoubleType, false),
    StructField("previous_result", DoubleType, true),
    StructField("mom", DoubleType, true),
    StructField("yoy", DoubleType, true)
  ))

  val TAG_FIRST: String = "1"
  val TAG_SECOND: String = "2"

  val TAG_DIM_FIRST: String = "NEW"
  val TAG_DIM_SECOND: String = "SECOND"

  /**
    * 累计成交量
    */
  val TAG_INDEX_DEAL_TOTAL_VOLUME: String = "TOTAL_DEAL_VOLUME"
  /**
    * 成交量
    */
  val TAG_INDEX_DEAL_VOLUME: String = "DEAL_VOLUME"
  /**
    * 成交均价
    */
  val TAG_INDEX_DEAL_PRICE_AVG: String = "DEAL_PRICE_AVG"
  /**
    * 挂牌量
    */
  val TAG_INDEX_QUOTED_VOLUME: String = "QUOTED_VOLUME"
  /**
    * 挂牌均价
    */
  val TAG_INDEX_QUOTED_PRICE_AVG: String = "QUOTED_PRICE_AVG"

  val TAG_OBJ_TYPE_CITY: String = "CITY"
  val TAG_OBJ_TYPE_DISTRICT: String = "DISTRICT"
  val TAG_OBJ_TYPE_PLATE: String = "PLATE"
  val TAG_OBJ_TYPE_HOUSE: String = "HOUSE"

  val TAG_PERIOD_TYPE_MONTH: String = "Month"
  val TAG_PERIOD_TYPE_PERIOD: String = "Period"
  val TAG_PERIOD_TYPE_TOTAL: String = "Total"

  val TAG_PERIOD_KEY_PERIOD: String = "period"
  val TAG_PERIOD_KEY_PERIOD_START: String = "s"
  val TAG_PERIOD_KEY_PERIOD_END: String = "e"
  val TAG_PERIOD_KEY_PRE_PERIOD_START: String = "p_s"
  val TAG_PERIOD_KEY_PRE_PERIOD_END: String = "p_e"

  val PERIOD_LIST = List(
    Map(TAG_PERIOD_KEY_PERIOD -> "Yesterday",
      TAG_PERIOD_KEY_PERIOD_START -> getBeforeOfDay(-1), TAG_PERIOD_KEY_PERIOD_END -> getBeforeOfDay(-1),
      TAG_PERIOD_KEY_PRE_PERIOD_START -> getBeforeOfDay(-2), TAG_PERIOD_KEY_PRE_PERIOD_END -> getBeforeOfDay(-2)),
    Map(TAG_PERIOD_KEY_PERIOD -> "SevenDays",
      TAG_PERIOD_KEY_PERIOD_START -> getBeforeOfDay(-7), TAG_PERIOD_KEY_PERIOD_END -> getBeforeOfDay(-1),
      TAG_PERIOD_KEY_PRE_PERIOD_START -> getBeforeOfDay(-14), TAG_PERIOD_KEY_PRE_PERIOD_END -> getBeforeOfDay(-8)),
    Map(TAG_PERIOD_KEY_PERIOD -> "ThirtyDays",
      TAG_PERIOD_KEY_PERIOD_START -> getBeforeOfDay(-30), TAG_PERIOD_KEY_PERIOD_END -> getBeforeOfDay(-1),
      TAG_PERIOD_KEY_PRE_PERIOD_START -> getBeforeOfDay(-60), TAG_PERIOD_KEY_PRE_PERIOD_END -> getBeforeOfDay(-31)),
    Map(TAG_PERIOD_KEY_PERIOD -> "SixtyDays",
      TAG_PERIOD_KEY_PERIOD_START -> getBeforeOfDay(-60), TAG_PERIOD_KEY_PERIOD_END -> getBeforeOfDay(-1),
      TAG_PERIOD_KEY_PRE_PERIOD_START -> getBeforeOfDay(-120), TAG_PERIOD_KEY_PRE_PERIOD_END -> getBeforeOfDay(-61)),
    Map(TAG_PERIOD_KEY_PERIOD -> "NinetyDays",
      TAG_PERIOD_KEY_PERIOD_START -> getBeforeOfDay(-90), TAG_PERIOD_KEY_PERIOD_END -> getBeforeOfDay(-1),
      TAG_PERIOD_KEY_PRE_PERIOD_START -> getBeforeOfDay(-180), TAG_PERIOD_KEY_PRE_PERIOD_END -> getBeforeOfDay(-91)),
    Map(TAG_PERIOD_KEY_PERIOD -> "SixMonths",
      TAG_PERIOD_KEY_PERIOD_START -> getBeforeOfDay(-180), TAG_PERIOD_KEY_PERIOD_END -> getBeforeOfDay(-1),
      TAG_PERIOD_KEY_PRE_PERIOD_START -> getBeforeOfDay(-360), TAG_PERIOD_KEY_PRE_PERIOD_END -> getBeforeOfDay(-181)),
    Map(TAG_PERIOD_KEY_PERIOD -> "OneYear",
      TAG_PERIOD_KEY_PERIOD_START -> getBeforeOfDay(-360), TAG_PERIOD_KEY_PERIOD_END -> getBeforeOfDay(-1),
      TAG_PERIOD_KEY_PRE_PERIOD_START -> getBeforeOfDay(-720), TAG_PERIOD_KEY_PRE_PERIOD_END -> getBeforeOfDay(-361))
  )

  object Period extends Enumeration {
    type Period = Value //声明枚举对外暴露的变量类型
    /**
      * 周期性统计（昨天，7天）
      */
    val Cyclical = Value("1")

    /**
      * 出仓月统计
      */
    val EjectMonth = Value("2")

    /**
      * 最后成交月统计
      */
    val DealMonth = Value("3")
  }

  def getPeriod(date: String): Map[String, String] = {
    val periodMap = PERIOD_LIST.filter(
      c => date >= c.getOrElse(App.TAG_PERIOD_KEY_PERIOD_START, "")
        && date <= c.getOrElse(App.TAG_PERIOD_KEY_PERIOD_END, ""))

    if (periodMap == null || periodMap.isEmpty) {
      return null
    }

    periodMap.head
  }

  /**
    *
    * 根据日期获取周期
    *
    * @param dateStr
    * @return
    */
  def getPeriodMonth(dateStr: String): Map[String, String] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(dateStr)

    getPeriodMonth(date, format)
  }


  /**
    *
    * 根据日期获取周期
    *
    * @param date
    * @return
    */
  def getPeriodMonth(date: Date): Map[String, String] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")

    getPeriodMonth(date, format)
  }

  /**
    *
    * @param date
    * @param format
    * @return
    */
  private def getPeriodMonth(date: Date, format: DateFormat): Map[String, String] = {
    var calendar = Calendar.getInstance()
    // 获取当月的第一天
    calendar.setTime(date)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    val currentFirstDay = format.format(calendar.getTime)
    calendar.add(Calendar.MONTH, -1)
    val previousFirstDay = format.format(calendar.getTime)

    // 获取当月的最后一天
    calendar.setTime(date)
    calendar.add(Calendar.MONTH, 1)
    calendar.set(Calendar.DAY_OF_MONTH, 0)
    val currentLastDay = format.format(calendar.getTime)
    calendar.add(Calendar.MONTH, -1)
    val previousLastDay = format.format(calendar.getTime)

    Map(TAG_PERIOD_KEY_PERIOD -> new SimpleDateFormat("yyyy-MM").format(date),
      TAG_PERIOD_KEY_PERIOD_START -> currentFirstDay, TAG_PERIOD_KEY_PERIOD_END -> currentLastDay,
      TAG_PERIOD_KEY_PRE_PERIOD_START -> previousFirstDay, TAG_PERIOD_KEY_PRE_PERIOD_END -> previousLastDay)
  }


  def markCityObject(city: String): String = "city:" + city

  def markDistrictObject(city: String, district: String): String = "city:" + city + ",district:" + district

  def markPlateObject(city: String, plate: String): String = "city:" + city + ",plate:" + plate

  def markHouseObject(city: String, house: String): String = "city:" + city + ",house:" + house

  /**
    * 获取昨天日期
    *
    * @return
    */
  def getLatestDay(): String = {
    getBeforeOfDay(-1)
  }

  /**
    * 获取今天日期
    *
    * @return
    */
  def getToday(): String = {
    getBeforeOfDay(0)
  }

  def getBeforeOfDay(interval: Int): String = {
    val calendar = Calendar.getInstance()
    calendar.add(Calendar.DAY_OF_YEAR, interval)
    new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime)
  }
}
