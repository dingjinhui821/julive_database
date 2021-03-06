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

outputDir=/dw/ods/yw_invoice
hdfs dfs -rm -r $outputDir

sqoop import \
--connect 'jdbc:mysql://172.18.24.200:3306/pc_comjia?tinyInt1isBit=false&characterEncoding=UTF-8&characterSetResults=UTF-8&zeroDateTimeBehavior=convertToNull' \
--username canal \
--password hungiG6ia=gho9Ioop9Ith*ne3Kie \
--query "select id,city_id,type,need_verify,company_id,transferee,expected_open_time,begin_title,tax_number,open_bank,accounts,register_address,telephone,img_list,content_type,content_note,note,status,invoice_number,invoice_open_time,tracking_number,invoice_applyer,invoice_apply_time,invalid_applyer,invalid_time,invalid_apply_time,invalid_reason,create_datetime,update_datetime,creator,updator,unit_type,unit_name,attachments,current_timestamp() as etl_time from pc_comjia.yw_invoice where \$CONDITIONS " \
--columns "id,city_id,type,need_verify,company_id,transferee,expected_open_time,begin_title,tax_number,open_bank,accounts,register_address,telephone,img_list,content_type,content_note,note,status,invoice_number,invoice_open_time,tracking_number,invoice_applyer,invoice_apply_time,invalid_applyer,invalid_time,invalid_apply_time,invalid_reason,create_datetime,update_datetime,creator,updator,unit_type,unit_name,attachments,etl_time" \
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
python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m "@15998885390: ODS??????(yw_invoice)ETL??????????????????????????????!"
eeooff
    exit $excode
fi

jobVersion=$(date +%s)_$RANDOM
indiValue=$(hive -e "select count(1) from ods.yw_invoice;")
endTime=$(date +%s)

if [ $excode -eq 0 ]; then
    mysql -uroot -pQemENw\>O0.Z -hoptimuspro01 -P3306 -e "insert into julive_dw.etl_task_running_log(task_name,job_version,start_time,end_time,exec_time,status,indi_type,indi_value,owner,etl_date) values('${fileName}','${jobVersion}',${startTime},${endTime},${endTime}-${startTime},1,1,'${indiValue}','ChavinKing','${dateId}')" 2>/dev/null 
else
    mysql -uroot -pQemENw\>O0.Z -hoptimuspro01 -P3306 -e "insert into julive_dw.etl_task_running_log(task_name,job_version,start_time,end_time,exec_time,status,indi_type,indi_value,owner,etl_date) values('${fileName}','${jobVersion}',${startTime},${endTime},${endTime}-${startTime},0,1,'${indiValue}','ChavinKing','${dateId}')" 2>/dev/null
fi
