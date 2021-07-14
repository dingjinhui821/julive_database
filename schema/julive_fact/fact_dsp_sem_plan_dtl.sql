drop table if exists julive_fact.fact_dsp_sem_plan_dtl;
CREATE EXTERNAL TABLE julive_fact.fact_dsp_sem_plan_dtl(
account_id               int    COMMENT '账户id', 
account_name             string COMMENT '账户', 
media_id                 bigint COMMENT '媒体类型ID', 
media_name               string COMMENT '媒体类型', 
module_id                bigint COMMENT '媒体形态ID', 
module_name              string COMMENT '媒体形态', 
plan_id                  bigint COMMENT '计划id:主键', 
plan_name                string COMMENT '计划名', 
budget                   double COMMENT '预算', 
plan_type                int    COMMENT '计划类型', 
device                   int    COMMENT '推广设备', 
negative_words           string COMMENT '否定关键词', 
negative_words_num       int    COMMENT '否定关键词个数', 
exact_negative_words     string COMMENT '精确否定关键词', 
exact_negative_words_num int    COMMENT '精确否定关键词个数', 
region_target            string COMMENT '推广区域', 
region_target_code       string COMMENT '推广区域码', 
pause                    int    COMMENT '是否暂停', 
price_ratio              double COMMENT '无线出价比例', 
pc_price_ratio           double COMMENT '计算机出价比例', 
bid_prefer               int    COMMENT '出价优先 1:计算机优先 2:移动优先', 
plan_status              int    COMMENT '推广计划状态 21-有效 22-处于暂停时段 23-暂停推广 24-推广计划预算不足 25-账户预算不足', 
show_prob                int    COMMENT '创意展现方式 1 - 优选 2 - 轮显', 
create_date              int    COMMENT '落仓时间', 
etl_time                 string COMMENT 'ETL跑数时间')
COMMENT '埋点明细表'
PARTITIONED BY ( 
  pdate string COMMENT '按照时间分区')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_dsp_sem_plan_dtl'
TBLPROPERTIES (
  'transient_lastDdlTime'='1573218830');
