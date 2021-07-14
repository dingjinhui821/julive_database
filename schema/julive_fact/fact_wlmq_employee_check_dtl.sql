CREATE EXTERNAL TABLE `julive_fact.fact_wlmq_employee_check_dtl`(
  `emp_check_id` bigint COMMENT '考勤id', 
  `emp_id` bigint COMMENT '员工id', 
  `emp_name` string COMMENT '员工ID', 
  `job_number` bigint COMMENT '员工工号', 
  `post_id` bigint COMMENT '岗位ID', 
  `post_name` string COMMENT '岗位名称', 
  `entry_date` string COMMENT '入职日期', 
  `dept_id` bigint COMMENT '部门ID', 
  `dept_name` string COMMENT '部门名称', 
  `city_id` bigint COMMENT '城市ID', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '带开城顺序的城市名称', 
  `check_date` string COMMENT '考勤日期:yyyy-MM-dd', 
  `weekday_zh` string COMMENT '星期中文:[星期一..星期日]', 
  `work_check_time` string COMMENT '上班打卡时间:yyyy-MM-dd HH:mm:ss', 
  `leave_check_time` string COMMENT '下班打卡时间:yyyy-MM-dd HH:mm:ss', 
  `plan_work_time` string COMMENT '排班上班时间:yyyy-MM-dd HH:mm:ss', 
  `plan_leave_time` string COMMENT '排卡下班时间:yyyy-MM-dd HH:mm:ss', 
  `status` int COMMENT '考勤结果:1正常 2外勤 3旷工 4迟到 5严重迟到 6早退 7休息 8休假 9迟到早退', 
  `plan_workday_num` float COMMENT '法定工作天数', 
  `real_workday_num` float COMMENT '出勤天数', 
  `etl_time` string COMMENT 'ETL跑数时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_wlmq_employee_check_dtl'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='221', 
  'rawDataSize'='4862', 
  'totalSize'='7684', 
  'transient_lastDdlTime'='1590511395')
