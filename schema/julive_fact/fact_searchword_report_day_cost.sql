CREATE EXTERNAL TABLE `julive_fact.fact_searchword_report_day_cost`(
  `creative_visit_url` string, 
  `update_datetime` string, 
  `click_rate` string, 
  `creative_title` string, 
  `creative_id` string, 
  `creative_desc2` string, 
  `creative_desc1` string, 
  `media_type` string, 
  `show_num` string, 
  `create_datetime` string, 
  `account_name` string, 
  `dsp_account_id` string, 
  `match_type` string, 
  `search_word` string, 
  `unit_id` string, 
  `keyword_name` string, 
  `default_visit_url` string, 
  `report_date` string, 
  `creator` string, 
  `cost` double, 
  `is_add_negative` string, 
  `creative_show_url` string, 
  `click_num` string, 
  `plan_name` string, 
  `is_add_keyword` string, 
  `average_ranking` string, 
  `unit_name` string, 
  `product_type` string, 
  `keyword_id` string, 
  `updator` string, 
  `average_click_price` string, 
  `plan_id` string, 
  `mobile_visit_url` string, 
  `device` string, 
  `bill_cost` string)
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
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_searchword_report_day_cost'
TBLPROPERTIES (
  'transient_lastDdlTime'='1574759981')
