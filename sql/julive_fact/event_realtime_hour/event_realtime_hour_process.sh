#!/bin/bash

start_date=`date +"%Y%m%d"`
end_date=`date -d next-day +"%Y%m%d"`

echo "开始日期:${start_date}"
echo "结束日期:${end_date}"

startSec=`date -d "$start_date" "+%s"`       #将开始日期和结束日期转换成秒的格式
endSec=`date -d "$end_date" "+%s"`

for((i=$startSec;i<$endSec;i+=3600))      #步长为24h
do
    current_day=`date -d "@$i" "+%Y%m%d-%H"`     #获得当前循环执行的日期
    
    hive -e "alter table julive_fact.fact_event_realtime_hour add partition (phour='${current_day}') location '/dw/julive_fact/fact_event_realtime_hour/${current_day}/';"

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
    ssh -p 10001 etl@test01 "python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m \"@18332118662, fact_event_realtime_hour增加分区 ${current_day} FAILED\""
    exit $EXCODE
fi

done
