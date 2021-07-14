#!/bin/bash
source /etc/profile
source ~/.bash_profile 
export HIVE_SKIP_SPARK_ASSEMBLY=true

startTime=`date +%s`

if [[ $# == 1 ]];then
    dateId=$1
elif [[ $# == 0 ]];then
    dateId=$(date -d "-1 day" +"%Y%m%d")
else
    echo "Usage sh "$0" dateId or sh "$0
    exit 1
fi

cd $(dirname "$0")
fileName=$(echo $0 | awk -F '/' '{print $NF}' | cut -d "." -f 1)

echo "Input parameter:dateId="$dateId
echo "Start exec "$0

outputDir=/dw/julive_dim/dim_city
hdfs dfs -rm -r $outputDir

sqoop import \
--connect 'jdbc:mysql://optimuspro01:3306/julive_dw?tinyInt1isBit=false&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=convertToNull' \
--username root \
--password QemENw\>O0.Z \
--query "select skey,city_id,city_name,city_seq,region,city_type,mgr_city,current_timestamp() as etl_time from julive_dw.dim_city where \$CONDITIONS " \
--columns "skey,city_id,city_name,city_seq,region,city_type,mgr_city,etl_time" \
--target-dir $outputDir \
--hive-drop-import-delims \
--fields-terminated-by '\001' \
--num-mappers 1

excode=$?
if [ $excode -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
else
    echo "********************************FAILED WITH EXITCODE $excode**********************************"
ssh -p10001 test01 <<eeooff
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15998885390: ODS采集(dim_city)ETL发生异常，请尽快处理!"
eeooff
    exit $excode
fi

jobVersion=$(date +%s)_$RANDOM
indiValue=$(hive -e "select count(1) from julive_dim.dim_city;")
endTime=$(date +%s)

if [ $excode -eq 0 ]; then
    mysql -uroot -pQemENw\>O0.Z -hoptimuspro01 -P3306 -e "insert into julive_dw.etl_task_running_log(task_name,job_version,start_time,end_time,exec_time,status,indi_type,indi_value,owner,etl_date) values('${fileName}','${jobVersion}',${startTime},${endTime},${endTime}-$
{startTime},1,1,'${indiValue}','ChavinKing','${dateId}')" 2>/dev/null 
else
    mysql -uroot -pQemENw\>O0.Z -hoptimuspro01 -P3306 -e "insert into julive_dw.etl_task_running_log(task_name,job_version,start_time,end_time,exec_time,status,indi_type,indi_value,owner,etl_date) values('${fileName}','${jobVersion}',${startTime},${endTime},${endTime}-$
{startTime},0,1,'${indiValue}','ChavinKing','${dateId}')" 2>/dev/null
fi
