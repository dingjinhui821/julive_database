#!/bin/bash
# get all filename in specified path

# cur_date="`date +%Y%m%d`"
# yes_date="`date -d last-day +%Y%m%d`"
# echo "当天日期:${cur_date}"
# echo "昨天日期:${yes_date}"

cur_day="`date +%Y%m%d`"
yes_date=`date -d "${cur_day} 1 days ago " "+%Y%m%d"`
before_yes_date=`date -d "${cur_day} 2 days ago " "+%Y%m%d"`


src_path=/data2/channel/day/${before_yes_date}
tar_path=/dw/julive_ods/src_keyword_baseinfo/${yes_date}
echo "源路径:${src_path}"
echo "目标路径:${tar_path}"

files=$(ls ${src_path})

#rm -rf filename.txt

for file in $files
do
 #echo ${file} >> filename.txt
 array=(${file//./ }) 
 file_path=${array[0]}

 echo "文件名:${file}"
 echo "目标路径:${tar_path}/${file_path}"

 file_size=$(ls -l $src_path/${file} | awk '{print $5}')
 echo $file_size
file_sizes=$(($file_size+0)) 
echo $file_sizes
if [[ $file_sizes -lt 100 ]]; 
then 
continue
#	echo "ttt"
else
 #echo "alter table julive_ods.src_keyword_baseinfo add partition (pdate='${yes_date}',file_name='${file_path}') location '${tar_path}/${file_path}/'"
 #echo "hdfs dfs -put ${src_path}/${file} ${tar_path}/${file_path}/"
	 hive -e "alter table julive_ods.src_keyword_baseinfo add partition (pdate='${yes_date}',file_name='${file_path}') location '${tar_path}/${file_path}/'"
hadoop fs -mkdir -p ${tar_path}/${file_path}/
 hdfs dfs -put ${src_path}/${file} ${tar_path}/${file_path}/
fi
done
#array=(${first_file_name//// })
#filename="${array[4]}"
#echo "文件名:${filename}"
EXCODE=$?
 if [ $EXCODE -eq 0 ]; then
    echo "********************************SUCCEED*********************************"
 else
    echo "********************************FAILED WITH EXITCODE $EXCODE**********************************"
     ssh -p 10001 etl@test01 "python /data4/nfsdata/dingtalk_monitor/monitor/azkaban_responder.py -m \"@15738713772,julive_ods.dim_keyword_infi_to_hdfs.sh FAILED\""
fi
