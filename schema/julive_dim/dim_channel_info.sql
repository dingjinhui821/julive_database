CREATE EXTERNAL TABLE `julive_dim.dim_channel_info`(
  `skey` string COMMENT '代理键', 
  `channel_id` int COMMENT '渠道id', 
  `channel_name` string COMMENT '渠道名称', 
  `city_id` int COMMENT '城市id', 
  `city_name` string COMMENT '城市名称', 
  `city_seq` string COMMENT '带开城顺序城市名称', 
  `media_id` int COMMENT '媒体类型ID', 
  `media_name` string COMMENT '媒体类型名称', 
  `module_id` int COMMENT '模块ID', 
  `module_name` string COMMENT '模块名称', 
  `device_id` int COMMENT '设备ID', 
  `device_name` string COMMENT '设备名称', 
  `app_type_id` int COMMENT 'APP类型 1 android 2 ios', 
  `app_type_name` string COMMENT 'APP类型 android ios', 
  `create_time` string COMMENT '创建时间', 
  `update_time` string COMMENT '更新时间', 
  `status` int COMMENT '状态 1 有效 0 无效', 
  `group_id` int COMMENT '渠道组ID 对应cj_agency.group_id', 
  `utm_source` string COMMENT '标记的广告来源', 
  `pdate` string COMMENT '分区日期', 
  `etl_time` string COMMENT 'etl跑数时间')
COMMENT '渠道维度表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_channel_info'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='2', 
  'numRows'='478011', 
  'rawDataSize'='10038231', 
  'totalSize'='31899243', 
  'transient_lastDdlTime'='1622392195')


