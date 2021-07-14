CREATE EXTERNAL TABLE `julive_app.offline_user_protrait_push_list_a`(
  `user_realname` string COMMENT '', 
  `city_id` int COMMENT '', 
  `user_mobile` string COMMENT '', 
  `user_id` string COMMENT '', 
  `probs` double COMMENT '', 
  `shanghu_time` string COMMENT '', 
  `order_id` string COMMENT '', 
  `intent_low_datetime` string COMMENT '', 
  `intent_low_reason` int COMMENT '', 
  `last_login_date` string COMMENT '用户姓名', 
  `is_huiliu` int, 
  `strategy_name` string, 
  `push_time` string, 
  `project_name` string, 
  `price` string, 
  `district_name` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\u0001', 
  'line.delim'='\n', 
  'serialization.format'='\u0001') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/offline_user_protrait_push_list_A'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'last_modified_by'='etl', 
  'last_modified_time'='1574178003', 
  'numFiles'='0', 
  'numRows'='5130', 
  'rawDataSize'='845464', 
  'totalSize'='0', 
  'transient_lastDdlTime'='1575100631')
 