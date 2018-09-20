#!/usr/bin/env bash
###################
# @author   vincent
# @cre_date 2018/4/20
# @contact  jisonglin@liyou.mobi
# @desc     创建dws_stats_quotation表
####################

sql = "create table IF NOT EXISTS liyou_test_db.dws_stats_quotation(
dim String COMMENT '分析的维度',
index String comment '分析的指标',
obj_type String comment '分析对象的类型',
obj_value String comment '分析的对象',
period_type String comment '分析区间的类型',
period_value String comment '分析区间',
current_result Double comment '本期数',
previous_result Double comment '上期数',
mom Double COMMENT '环比',
yoy Double COMMENT '同比')
comment '小区成交数据'
PARTITIONED BY (ds String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';"

hive --database liyou_test_db -e "${sql}"