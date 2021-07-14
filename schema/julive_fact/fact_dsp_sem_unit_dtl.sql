CREATE EXTERNAL TABLE `julive_fact.fact_dsp_sem_unit_dtl`(
  `account_id` bigint COMMENT '账户id', 
  `account_name` string COMMENT '账户', 
  `media_id` bigint COMMENT '媒体类型ID', 
  `media_name` string COMMENT '媒体类型', 
  `module_id` bigint COMMENT '媒体形态ID', 
  `module_name` string COMMENT '媒体形态', 
  `plan_id` bigint COMMENT '计划id', 
  `plan_name` string COMMENT '计划名', 
  `unit_id` bigint COMMENT '单元id:主键', 
  `unit_name` string COMMENT '计划名', 
  `max_price` double COMMENT '最高出价', 
  `pause` int COMMENT '是否暂停', 
  `negative_words` string COMMENT '否定关键词', 
  `negative_words_num` int COMMENT '否定关键词个数', 
  `exact_negative_words` string COMMENT '精确否定关键词', 
  `exact_negative_words_num` int COMMENT '精确否定关键词个数', 
  `unit_status` int COMMENT '状态 31-有效 32-暂停推广 33-推广计划暂停推广', 
  `price_ratio` double COMMENT '无线出价比例', 
  `pc_price_ratio` double COMMENT '计算机出价比例', 
  `accu_price_factor` double COMMENT '精确出价比例', 
  `wide_price_factor` double COMMENT '广泛出价比例', 
  `word_price_factor` double COMMENT '短语出价比例', 
  `segment_recommend_status` int COMMENT '图片素材配图开关 1 – 关闭 0 – 开启，默认开启', 
  `match_price_status` int COMMENT '分匹配状态 0 开启，要求精确系数>= 短语系数>=广泛系数，且三个比例系数均不能为空 1关', 
  `create_date` string COMMENT '落仓时间', 
  `etl_time` string COMMENT 'ETL跑数时间')
COMMENT '单元事实表'
PARTITIONED BY ( 
  `pdate` string COMMENT '数据日期:yyyyMMdd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_dsp_sem_unit_dtl'
TBLPROPERTIES (
  'transient_lastDdlTime'='1573190656')
