drop table if exists tmp_bi.tmp_clue_beijing_yesterday;
create table tmp_bi.tmp_clue_beijing_yesterday(
clue_id                                           bigint                  comment '线索ID',
user_mobile                                       string                  comment '用户手机号',
create_date                                       string                  comment '创建日期:yyyy-MM-dd'
) comment '北京昨天线索'
PARTITIONED BY ( 
  `pdate` string)
stored as parquet;


