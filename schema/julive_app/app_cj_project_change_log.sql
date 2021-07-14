CREATE TABLE `julive_app.app_cj_project_change_log`(
  `project_id` bigint, 
  `name` string, 
  `current_rate` bigint, 
  `current_price_type` string, 
  `price_type` string, 
  `price_min` bigint, 
  `price_max` bigint, 
  `acreage_min` bigint, 
  `acreage_max` bigint, 
  `status` string, 
  `project_type` string, 
  `district_id` string, 
  `trade_area` string, 
  `developer` string, 
  `school_district_room` string, 
  `near_subway` string, 
  `is_cooperate` string, 
  `is_outreach` string, 
  `lng` string, 
  `lat` string, 
  `good_decorate` string, 
  `open_time_year` string, 
  `open_time_month` string, 
  `open_time_day` string, 
  `open_time_ten` string, 
  `brand_developer` string)
PARTITIONED BY ( 
  `pdate` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_cj_project_change_log'
TBLPROPERTIES (
  'transient_lastDdlTime'='1585192204')
 
