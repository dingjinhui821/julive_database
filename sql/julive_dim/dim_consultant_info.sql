set etl_date = '${hiveconf:etlDate}'; -- 取昨天日期
set spark.app.name=dim_consultant_info;
set mapred.job.name=dim_consultant_info;

set hive.execution.engine=spark;
set mapreduce.job.queuename=root.etl;
set spark.yarn.queue=etl;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;



insert overwrite table julive_dim.dim_consultant_info partition(pdate)

select *
from julive_dim.dim_employee_base_info
where pdate =${hiveconf:etl_date}
and from_source = 1
and post_id in (132,164,261,133,204,260,162,258,131,163,259)
and emp_name not like '%测试%'
;

