CREATE EXTERNAL TABLE `julive_app.offline_user_protrait`(
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
  `project_name` string, 
  `price` string, 
  `district_name` string, 
  `sex` string, 
  `more_mobile` string, 
  `channel_id` string, 
  `customer_intent_city` string, 
  `source` string, 
  `device_type` string, 
  `is_short_alloc` string, 
  `op_type` string, 
  `project_ids` string)
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
  'hdfs://optimuspro01:8020/dw/julive_app/offline_user_protrait'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='8', 
  'numRows'='49354', 
  'rawDataSize'='10269292', 
  'totalSize'='10318646', 
  'transient_lastDdlTime'='1590545620')
 