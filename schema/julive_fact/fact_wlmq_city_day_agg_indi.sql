CREATE EXTERNAL TABLE `julive_fact.fact_wlmq_city_day_agg_indi`(
  `date_str` string COMMENT '日期字符串:yyyy-MM-dd', 
  `date_str_zh` string COMMENT '日期中文:yyyy年MM月dd日', 
  `city_id` bigint COMMENT '城市id', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '带开城顺序城市名称', 
  `city_region` string COMMENT '所属大区', 
  `city_type` string COMMENT '城市类型：新城 老城_含副区', 
  `mgr_city` string COMMENT '主城', 
  `clue_num` int COMMENT '日线索量', 
  `distribute_num` int COMMENT '日上户量', 
  `distribute_day_num` int COMMENT '日发生上户日期总数', 
  `see_num` int COMMENT '日带看量', 
  `see_project_num` int COMMENT '日带看楼盘量', 
  `subscribe_contains_cancel_ext_num` int COMMENT '日认购量-含退、含外联', 
  `subscribe_contains_cancel_ext_amt` decimal(19,4) COMMENT '日认购-含退、含外联GMV', 
  `subscribe_contains_cancel_ext_income` decimal(19,4) COMMENT '日认购-含退、含外联收入', 
  `subscribe_contains_cancel_ext_project_num` int COMMENT '日认购楼盘量-含退、含外联(月破蛋楼盘数)', 
  `subscribe_contains_ext_amt` decimal(19,4) COMMENT '日认购-含外联GMV', 
  `subscribe_contains_ext_income` decimal(19,4) COMMENT '日认购-含外联收入', 
  `subscribe_cancel_contains_ext_amt` decimal(19,4) COMMENT '日退认购金额-含外联', 
  `subscribe_contains_ext_num` int COMMENT '日认购量-含外联', 
  `subscribe_cancel_contains_ext_num` int COMMENT '日退认购量-含外联', 
  `subscribe_cancel_contains_ext_income` decimal(19,4) COMMENT '日退认购佣金-含外联', 
  `subscribe_coop_num` int COMMENT '净认购量', 
  `sign_contains_cancel_ext_num` int COMMENT '日签约量-含退、含外联', 
  `sign_contains_cancel_ext_income` decimal(19,4) COMMENT '日签约-含退、含外联收入', 
  `sign_contains_ext_income` decimal(19,4) COMMENT '日签约-不含退、含外联收入', 
  `sign_contains_ext_num` int COMMENT '日签约量-含外联', 
  `sign_cancel_contains_ext_num` int COMMENT '日退签约量-含外联', 
  `sign_coop_num` int COMMENT '净签约量', 
  `actual_amt` decimal(19,4) COMMENT '日回款金额', 
  `real_workday_num` int COMMENT '员工出勤天数', 
  `etl_time` string COMMENT 'ETL跑数时间')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_wlmq_city_day_agg_indi'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='88', 
  'rawDataSize'='2904', 
  'totalSize'='7188', 
  'transient_lastDdlTime'='1590544553')
