CREATE TABLE `julive_app.app_appinstall_product_media_day`(
  `create_date` string COMMENT '创建日期:yyyy-MM-dd', 
  `product_id` int COMMENT '产品端', 
  `city_id` int COMMENT '城市ID', 
  `city_name` string COMMENT '城市名称', 
  `media_name` string COMMENT '媒体名称', 
  `clue_num` int COMMENT '产生线索的线索数', 
  `distribute_num` int COMMENT '产生上户的线索数', 
  `clue_see_num` int COMMENT '产生带看的线索数', 
  `see_num` int COMMENT '带看量', 
  `clue_subscribe_num` int COMMENT '产生认购的线索数', 
  `subscribe_contains_cancel_ext_num` int COMMENT '认购量', 
  `clue_sign_num` int COMMENT '产生签约的线索数', 
  `sign_contains_cancel_ext_num` int COMMENT '签约量', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT 'APP Install时间维度业务指标表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_app/app_appinstall_product_media_day'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='41342', 
  'rawDataSize'='578788', 
  'totalSize'='252491', 
  'transient_lastDdlTime'='1590544918')
