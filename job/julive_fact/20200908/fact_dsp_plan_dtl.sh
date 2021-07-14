#!/bin/bash
source /etc/profile
source ~/.bash_profile 

basedir=`cd \`dirname $0\`\/..; pwd`
pdate=$(date -d "1 day ago" +"%Y%m%d")

/usr/bin/hive -hiveconf pdate=$pdate  -f $basedir/julive_dim/map_dsp_acctid_2_acctname.sql 

EXCODE=$?
if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
ssh -p10001 test01 <<eeooff
python /data4/dw/dingtalk_monitor/monitor/azkaban_responder.py -m "@15998885390: dsp账户id和名称映射表(map_dsp_acctid_2_acctname)ETL发生异常，请尽快处理!"
eeooff
    exit $EXCODE
fi