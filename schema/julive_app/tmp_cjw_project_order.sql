CREATE TABLE `julive_app.tmp_cjw_project_order`(
  `order_time` string, 
  `project_id` string, 
  `sum_order` bigint, 
  `city_id` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/tmp_cjw_project_order'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='215186', 
  'rawDataSize'='6354763', 
  'totalSize'='6569949', 
  'transient_lastDdlTime'='1590524318')

