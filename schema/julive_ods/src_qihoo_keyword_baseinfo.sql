drop table if exists julive_ods.src_qihoo_keyword_baseinfo;
CREATE EXTERNAL TABLE `julive_ods.src_qihoo_keyword_baseinfo`(
  `keyword_id` string COMMENT '关键词id', 
  `account_id` string COMMENT '账户ID', 
  `account_name` string COMMENT '账户名称', 
  `plan_id` string COMMENT '计划ID', 
  `plan_name` string COMMENT '计划名称', 
  `plan_cost` double COMMENT '推广计划每日预算', 
  `plan_device` string COMMENT '推广计划设备类型', 
  `plan_status` string COMMENT '推广计划状态', 
  `plan_system_status` string COMMENT '推广计划系统状态', 
  `unit_id` string COMMENT '推广组ID', 
  `unit_name` string COMMENT '推广组名称', 
  `unit_bid` string COMMENT '推广组出价', 
  `unit_status` string COMMENT '推广组状态', 
  `unit_system_status` string COMMENT '推广组系统状态', 
  `negative_words` string COMMENT '计划否定关键词', 
  `exact_negative_words` string COMMENT '计划精确否定关键词', 
  `groupnegative_words` string COMMENT '组否定关键词', 
  `groupexact_negative_words` string COMMENT '组精确否定关键词', 
  `keyword_name` string COMMENT '关键词', 
  label          string COMMENT '标签',
  `match_type` string COMMENT '关键词匹配模式', 
  `price` string COMMENT '关键词出价', 
  `pause` string COMMENT '关键词状态', 
  `status` string COMMENT '关键词系统状态', 
  `shift_status` string COMMENT '关键词移动状态', 
  `suggest_splowest` string COMMENT '关键词建议最低起价', 
  `originality_headline` string COMMENT '创意标题', 
  `originality_describefirst` string COMMENT '创意描述1', 
  `originality_describesecond` string COMMENT '创意描述2', 
  `originality_status` string COMMENT '创意状态', 
  `originality_system_status` string COMMENT '创意系统状态', 
  `originality_shift_status` string COMMENT '创意移动状态', 
  `show_url` string COMMENT '显示网址', 
  `pc_url` string COMMENT '链接网址', 
  `plan_area` string COMMENT '推广计划推广地域', 
  `seek_area_intention` string COMMENT '搜索地域意图定位', 
  `advanced_match_type` string COMMENT '高级精确匹配', 
  `plan_time_frame` string COMMENT '推广计划推广时段', 
  `plan_begin_date` string COMMENT '推广计划开始时间', 
  `plan_finish_date` string COMMENT '推广计划结束时间', 
  `create_time` string COMMENT '创建时间', 
  `update_time` string COMMENT '修改时间', 
  `pc_quality` string COMMENT '关键词质量度', 
  `m_quality` string COMMENT '移动质量度', 
  `material_type` string COMMENT '物料类型', 
  `m_url` string COMMENT '关键词移动链接网址', 
  `originality_shift_pc_url` string COMMENT '创意移动显示网址', 
  `originality_shift_link_url` string COMMENT '创意移动链接网址', 
  `shift_seek_bid_ratio` string COMMENT '移动搜索出价比例', 
  `one_call` string COMMENT '电话直呼', 
  `phoenix_dance_content` string COMMENT '凤舞内容', 
  `error_message` string COMMENT '错误信息')
COMMENT '360关键词基础信息数据'
PARTITIONED BY ( 
  `pdate` string COMMENT '分区日期', 
  `file_name` string COMMENT '文件名')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'=',', 
  'serialization.encoding'='GB2312', 
  'skip.header.line.count'='1') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_ods/src_qihoo_keyword_baseinfo'
TBLPROPERTIES (
  'transient_lastDdlTime'='1574834426')