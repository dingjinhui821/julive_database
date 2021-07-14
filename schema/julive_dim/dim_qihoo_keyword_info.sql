CREATE TABLE `julive_dim.dim_qihoo_keyword_info`(
  `skey` string COMMENT '主键', 
  `create_date` string COMMENT '日期', 
  `media_id` int COMMENT '媒体id', 
  `module_id` int COMMENT '类型 id', 
  `module_name` string COMMENT '类型', 
  `account_id` int COMMENT '账户 id', 
  `plan_id` bigint COMMENT '计划 ID', 
  `unit_id` bigint COMMENT '单元 id', 
  `keyword_id` bigint COMMENT '关键词 ID', 
  `keyword_name` string COMMENT '关键词名称', 
  `price` decimal(15,4) COMMENT '关键词出价', 
  `price_old` decimal(15,4) COMMENT '前一天关键词出价', 
  `match_type` int COMMENT '匹配模式值', 
  `match_type_old` int COMMENT '前一天匹配模式值', 
  `match_type_desc` string COMMENT '匹配模式名称', 
  `status` int COMMENT '关键词状态值', 
  `status_old` int COMMENT '前一天关键词状态值', 
  `status_desc` string COMMENT '关键词状态名称', 
  `pause` int COMMENT '暂停/启用关键词', 
  `pause_old` int COMMENT '前一天 暂停/启用关键词', 
  `pc_url` string COMMENT 'pc 目标 url', 
  `m_url` string COMMENT '移动访问 url', 
  `wmatchprefer` string COMMENT '是否接受单元的分匹配出价比例', 
  `wmatchprefer_old` string COMMENT '前一天是否接受单元的分匹配出价比例', 
  `pc_quality` string COMMENT 'pc 上该关键词的10评分质量度', 
  `pc_reliable` string COMMENT 'pc 上新广告维度下是否为临时质量度', 
  `pc_reason` string COMMENT 'pc 上质量度原因', 
  `m_quality` string COMMENT '移动上该关键词的10分质量度', 
  `m_reliable` string COMMENT '移动上新广告维度下是否为临时质量度', 
  `m_reason` string COMMENT '移动质量度原因', 
  `pc_short_url` string COMMENT '短pcurl', 
  `m_short_url` string COMMENT '短murl', 
  `pc_channel_id` string COMMENT 'pc_channel_id', 
  `m_channel_id` string COMMENT 'm_channel_id')
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
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_qihoo_keyword_info'
TBLPROPERTIES (
  'EXTERNAL'='false', 
  'last_modified_by'='etl', 
  'last_modified_time'='1589016327', 
  'transient_lastDdlTime'='1589016327')

