CREATE EXTERNAL TABLE `julive_dim.dim_optype`(
  `skey` string COMMENT '部门表代理键:主键', 
  `op_type_id` int COMMENT 'op_type 的值', 
  `op_type_name` string COMMENT 'op_type 的英文名称', 
  `op_type_name_cn` string COMMENT '描述', 
  `site` int COMMENT '渠道来源：0: 无  1: pc  2: m  3: pc_m  4: app  5: other', 
  `site_ts` int COMMENT '渠道来源：0: 无  1: pc  2: m  3: pc_m  4: app  5: other', 
  `from_page` string COMMENT '所在页面', 
  `from_module` string COMMENT '所属模块', 
  `status` int COMMENT '0:删除1:正常', 
  `is_leave_mobile` int COMMENT '是否留电 1是 2否', 
  `create_time` string COMMENT '创建时间', 
  `update_time` string COMMENT '更新时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '留电口维度表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.format'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_optype'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='1503', 
  'rawDataSize'='265416', 
  'totalSize'='266919', 
  'transient_lastDdlTime'='1590510015')

