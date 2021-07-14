CREATE EXTERNAL TABLE `julive_app.project_search_tag_hot`(
  `city_id` string, 
  `district` string, 
  `district_text` string, 
  `whole_price` string, 
  `price_text` string, 
  `house_type` string, 
  `house_type_text` string, 
  `project_type` string, 
  `project_type_text` string, 
  `features_first` string, 
  `features_text_first` string, 
  `features_second` string, 
  `features_text_second` string)
PARTITIONED BY ( 
  `pdate` string)
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
  'hdfs://optimuspro01:8020/dw/julive_app/project_search_tag_hot'
TBLPROPERTIES (
  'transient_lastDdlTime'='1570872828')

