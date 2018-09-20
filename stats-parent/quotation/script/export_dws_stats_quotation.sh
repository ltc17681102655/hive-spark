#!/usr/bin/env bash
######################################
# @author   vincent
# @cre_date 2018/4/20
# @contact  jisonglin@liyou.mobi
# @desc     创建dws_stats_quotation表
#######################################
ds=`date -d "1 day ago" "+%Y-%m-%d"`

sqoop export \
--connect jdbc:mysql://mysql.test.tuboshi.co:3306/sHouseApp_pre?useSSL=false \
--username tbs \
--password LangBoshi^666 \
--columns dim,index,obj_type,obj_value,period_type,period_value,current_result,previous_result,mom,yoy \
--num-mappers 1 \
--table=stats_quotation \
--export-dir=/apps/hive/warehouse/liyou_test_db.db/dws_stats_quotation/ds=$ds \
--input-fields-terminated-by '\t' \
--input-null-string '\\N' \
--input-null-non-string '\\N';