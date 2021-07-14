CREATE EXTERNAL TABLE `julive_dim.dim_date`(
  `skey` string COMMENT '代理键：主键:yyyyMMdd', 
  `date_id` int COMMENT '日期字符串整数格式:yyyyMMdd', 
  `date_str` string COMMENT '日期字符串:yyyy-MM-dd', 
  `date_str_zh` string COMMENT '日期中文:yyyy年MM月dd日', 
  `date_of_month` int COMMENT '当月第n天:1-31', 
  `date_of_month_zh` string COMMENT '当月第n天中文:当月第[1-31]天', 
  `date_of_year` int COMMENT '当年第n天:1-365', 
  `date_of_year_zh` string COMMENT '当年第n天中文:当年第[1-365]天', 
  `week_id` int COMMENT '周标识:1-7', 
  `week_zh` string COMMENT '周中文:[周一..周日]', 
  `weekday_zh` string COMMENT '星期中文:[星期一..星期日]', 
  `week_of_year` string COMMENT '当年第n周:当年第N周', 
  `week_desc` string COMMENT '周描述:yyyy年第N周', 
  `week_range` string COMMENT '周范围:yyyy年第N周(MM/dd~MM/dd)', 
  `week_type` string COMMENT '周中:周一..周五 周末:周六 周日', 
  `ten_id` int COMMENT '旬id:1 上旬 2 中旬 3 下旬', 
  `ten_zh` string COMMENT '旬中文:上旬 中旬 下旬', 
  `month_id` int COMMENT '月份标识:yyyyMM', 
  `month_no` int COMMENT '月份编号:1-12', 
  `month_zh` string COMMENT '月份中文:01月', 
  `yearmonth` string COMMENT '年月标识:yyyy-MM', 
  `yearmonth_zh` string COMMENT '年月中文:2010年01月', 
  `qr_id` int COMMENT '季度标识:1 Q1 2 Q2 3 Q3 4 Q4', 
  `qr_en` string COMMENT '季度英文:Q1 Q2 Q3 Q4', 
  `qr_zh` string COMMENT '季度中文:第一季度 第二季度 第三季度 第四季度', 
  `year_id` int COMMENT '年标识:yyyy', 
  `year_zh` string COMMENT '年中文:yyyy年')
COMMENT '日期维度表'
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
  'hdfs://optimuspro01:8020/dw/julive_dim/dim_date'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='7670', 
  'rawDataSize'='1875462', 
  'totalSize'='1883132', 
  'transient_lastDdlTime'='1571724167')

