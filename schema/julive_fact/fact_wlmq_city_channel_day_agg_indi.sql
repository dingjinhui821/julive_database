CREATE EXTERNAL TABLE `julive_fact.fact_wlmq_city_channel_day_agg_indi`(
  `date_str` string COMMENT '日期字符串:yyyy-MM-dd', 
  `date_str_zh` string COMMENT '日期中文:yyyy年MM月dd日', 
  `week_type` string COMMENT '周中 周末', 
  `city_id` bigint COMMENT '城市id', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '带开城顺序的城市名称', 
  `channel_id` bigint COMMENT '渠道ID', 
  `channel_name` string COMMENT '渠道名称', 
  `media_id` int COMMENT '媒体类型ID', 
  `media_name` string COMMENT '媒体类型名称', 
  `module_id` int COMMENT '模块ID', 
  `module_name` string COMMENT '模块名称', 
  `device_id` int COMMENT '设备ID', 
  `device_name` string COMMENT '设备名称', 
  `clue_num` int COMMENT '线索量', 
  `distribute_num` int COMMENT '上户量', 
  `see_num` int COMMENT '带看量', 
  `see_project_num` int COMMENT '带看楼盘量', 
  `subscribe_contains_cancel_ext_num` int COMMENT '认购量-含退、含外联', 
  `subscribe_contains_cancel_ext_amt` decimal(19,4) COMMENT '认购-含退、含外联GMV', 
  `subscribe_contains_cancel_ext_income` decimal(19,4) COMMENT '认购-含退、含外联收入', 
  `subscribe_contains_cancel_ext_project_num` int COMMENT '认购楼盘量-含退、含外联(月破蛋楼盘数)', 
  `subscribe_contains_ext_amt` decimal(19,4) COMMENT '认购-含外联GMV', 
  `subscribe_contains_ext_income` decimal(19,4) COMMENT '认购-含外联收入', 
  `subscribe_cancel_contains_ext_amt` decimal(19,4) COMMENT '退认购金额-含外联', 
  `subscribe_contains_ext_num` int COMMENT '认购量-含外联', 
  `subscribe_cancel_contains_ext_num` int COMMENT '退认购量-含外联', 
  `subscribe_cancel_contains_ext_income` decimal(19,4) COMMENT '退认购佣金-含外联', 
  `sign_contains_cancel_ext_num` int COMMENT '签约量-含退、含外联', 
  `sign_contains_cancel_ext_income` decimal(19,4) COMMENT '签约-含退、含外联收入', 
  `sign_contains_ext_num` int COMMENT '签约量-含外联', 
  `sign_cancel_contains_ext_num` int COMMENT '退签约量-含外联', 
  `etl_time` string COMMENT 'ETL跑数时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_wlmq_city_channel_day_agg_indi'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='190', 
  'rawDataSize'='6270', 
  'totalSize'='11852', 
  'transient_lastDdlTime'='1590544666')
