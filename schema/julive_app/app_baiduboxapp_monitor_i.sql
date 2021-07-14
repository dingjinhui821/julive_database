CREATE EXTERNAL TABLE `julive_app.app_baiduboxapp_monitor_i`(
  `user_agent` string, 
  `baiduboxapp` string)
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_baiduboxapp_monitor_i'
TBLPROPERTIES (
  'transient_lastDdlTime'='1574927400')
 