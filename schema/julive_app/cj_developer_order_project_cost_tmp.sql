CREATE TABLE `julive_app.cj_developer_order_project_cost_tmp`(
  `id` string, 
  `order_time` string, 
  `city_id` bigint, 
  `developer_id` bigint, 
  `project_id` bigint, 
  `source` bigint, 
  `employee_id` bigint, 
  `user_mobile` bigint, 
  `channel_id` bigint, 
  `user_id` bigint, 
  `project_cost` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/cj_developer_order_project_cost_tmp'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='18', 
  'rawDataSize'='1244', 
  'totalSize'='1262', 
  'transient_lastDdlTime'='1590470500')
 
