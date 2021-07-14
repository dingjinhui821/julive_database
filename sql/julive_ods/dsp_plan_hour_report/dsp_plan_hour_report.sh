#!/bin/bash
#source /data1/etl/.bash_profile
source /etc/profile
for((i=5;i<=56;i++))
do
echo $i
DATE_ID=$(date -d "-$i day" +%Y%m%d )


DATE_ID_FORMAT=$(date -d "$DATE_ID" +%Y-%m-%d )

echo $DATE_ID_FORMAT

hive -hivevar DATE_ID=${DATE_ID} -f /data4/nfsdata/julive_dw/etl/sql/julive_ods/dsp_plan_hour_report/dsp_plan_hour_report.sql


EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
    ssh -p 10001 etl@test01 "python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m \"@15738713772,ods.dsp_plan_hour_report FAILED\""   
exit $EXCODE
fi
done
