#!/bin/bash

DATE_ID=`date -d "2 day ago" +"%Y%m%d"`

echo "当前删除的日期为 : ${DATE_ID}"

hdfs dfs -rm -r /dw/julive_fact/fact_event_realtime_hour/${DATE_ID}*

