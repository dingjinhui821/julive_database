CREATE TABLE `julive_app.app_click_search_result_uvpv`(
  `project_id` string, 
  `select_city` string, 
  `global_id` string, 
  `pv` string)
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_click_search_result_uvpv'
TBLPROPERTIES (
  'transient_lastDdlTime'='1574264222')
 