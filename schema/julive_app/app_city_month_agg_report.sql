CREATE TABLE `julive_app.app_city_month_agg_report`(
  `yearmonth` string COMMENT '年月标识:yyyy-MM', 
  `yearmonth_zh` string COMMENT '年月中文:2010年01月', 
  `city_id` bigint COMMENT '城市id', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '带开城顺序的城市名称', 
  `city_region` string COMMENT '所属大区', 
  `city_type` string COMMENT '城市类型：新城 老城', 
  `clue_num` int COMMENT '月/年 线索量', 
  `distribute_num` int COMMENT '月/年 上户量', 
  `distribute_day_num` int COMMENT '月/年 发生上户日期总数', 
  `see_num` int COMMENT '月/年 带看量', 
  `see_project_num` int COMMENT '月/年 带看楼盘量', 
  `subscribe_contains_cancel_ext_num` int COMMENT '月/年 认购量-含退、含外联', 
  `subscribe_contains_cancel_ext_amt` decimal(19,4) COMMENT '月/年 认购-含退、含外联GMV', 
  `subscribe_contains_cancel_ext_income` decimal(19,4) COMMENT '月/年 认购-含退、含外联收入', 
  `subscribe_contains_cancel_ext_project_num` int COMMENT '月/年 认购楼盘量-含退、含外联(月破蛋楼盘数)', 
  `subscribe_contains_ext_amt` decimal(19,4) COMMENT '月/年 认购-含外联GMV', 
  `subscribe_contains_ext_income` decimal(19,4) COMMENT '月/年 认购-含外联收入', 
  `subscribe_cancel_contains_ext_amt` decimal(19,4) COMMENT '月/年 退认购金额-含外联', 
  `subscribe_contains_ext_num` int COMMENT '月/年 认购量-含外联', 
  `subscribe_cancel_contains_ext_num` int COMMENT '月/年 退认购量-含外联', 
  `subscribe_cancel_contains_ext_income` decimal(19,4) COMMENT '月/年 退认购佣金-含外联', 
  `subscribe_coop_num` int COMMENT '月/年 净认购量', 
  `sign_contains_cancel_ext_num` int COMMENT '月/年 签约量-含退、含外联', 
  `sign_contains_cancel_ext_income` decimal(19,4) COMMENT '月/年 签约-含退、含外联收入', 
  `sign_contains_ext_num` int COMMENT '月/年 签约量-含外联', 
  `sign_cancel_contains_ext_num` int COMMENT '月/年 退签约量-含外联', 
  `sign_coop_num` int COMMENT '月/年 净签约量', 
  `actual_amt` decimal(19,4) COMMENT '月/年 日回款金额', 
  `real_workday_num` int COMMENT '月/年 员工出勤天数', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '月/年-城市报表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_city_month_agg_report'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='885', 
  'rawDataSize'='27435', 
  'totalSize'='74709', 
  'transient_lastDdlTime'='1590545021')
 
