#!/bin/bash

cur_day="`date +%Y%m%d`"
yes_date=`date -d "${cur_day} 1 days ago " "+%Y%m%d"`
before_yes_date=`date -d "${cur_day} 2 days ago " "+%Y%m%d"`

src_path=/data2/channel_report_hour/fail/${before_yes_date}
tar_path=/dw/julive_ods/channel_report_hour/${yes_date}
echo "源路径:${src_path}"
echo "目标路径:${tar_path}"

files=$(ls ${src_path})

for file in $files
do

 array=(${file//./ }) 
 file_path=${array[0]}

 echo "文件名:${file}"
 echo "目标路径:${tar_path}/${file_path}"
 hadoop fs -mkdir -p ${tar_path}/${file_path}
 hdfs dfs -put ${src_path}/${file} ${tar_path}/${file_path}/

done
EXCODE=$?
 if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
 else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
     ssh -p 10001 etl@test01 "python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m \"@15738713772,上传channel_report_day数据失败 FAILED\""
fi
