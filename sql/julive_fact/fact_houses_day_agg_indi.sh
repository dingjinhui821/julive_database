#!/bin/bash
source /etc/profile
source ~/.bash_profile
/usr/bin/hive -f /data4/nfsdata/julive_dw/etl/sql/julive_fact/fact_houses_day_agg_indi.sql

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/dw/dingtalk_monitor/monitor/azkaban_responder.py -m "@15738713772: etl表(fact_houses_day_agg_indi)ETL发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi
