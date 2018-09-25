#!/usr/bin/env bash

ds=2018-09-21

sqoop export \
--connect jdbc:mysql://mysql.test.tuboshi.co:3306/sHouseApp_pre?useSSL=false \
--username tbs \
--password LangBoshi^666 \
--columns dim,index,obj_type,obj_value,period_type,period_value,current_result,previous_result,mom,yoy \
--num-mappers 1 \
--table=ltc_stats_quotation \
--export-dir=/apps/hive/warehouse/liutongchun.db/dws_stats_quotation/ds=2018-09-21 \
--input-fields-terminated-by '\t' \
--input-null-string '\\N' \
--input-null-non-string '\\N';