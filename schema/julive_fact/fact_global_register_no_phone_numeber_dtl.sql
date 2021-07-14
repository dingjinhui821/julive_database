drop table if exists julive_fact.fact_global_register_no_phone_numeber_dtl;
create external table julive_fact.fact_global_register_no_phone_numeber_dtl(
global_id            string COMMENT '全局用户标识', 
user_id              string COMMENT '神策USER_ID', 
julive_id            string COMMENT '居理用户ID', 
comjia_unique_id     string COMMENT '用户ID', 
product_id           string COMMENT '产品ID',
user_mobile          string comment '用户手机号',
create_time          string COMMENT '创建时间',
first_register_date  string COMMENT '注册时间',
etl_time             string comment '跑数时间'
)
comment'用户注册时间'
partitioned by (pdate string comment '分区表')
STORED AS TEXTFILE;