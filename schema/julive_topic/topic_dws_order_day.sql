-- TOPIC-DWS-订单业务指标表

drop table if exists julive_topic.topic_dws_order_day;
create table if not exists julive_topic.topic_dws_order_day(
clue_id                               bigint          comment '线索ID',
report_date                           string          comment '业务日期:yyyy-MM-dd',
clue_num                              int             comment '线索量',
clue_num_400                          int             comment '400线索量',
distribute_num                        int             comment '上户量',
distribute_num_400                    int             comment '400上户量',
see_num                               int             comment '带看量',
see_num_online                        int             comment '线上带看数',
subscribe_num                         int             comment '认购量:含退、含外联',
subscribe_contains_ext_num            int             comment '认购量(不含退)-含外联',
subscribe_coop_num                    int             comment '净认购量:不含退、不含外联',
subscribe_contains_cancel_ext_income  decimal(19,4)   comment '认购-含退、含外联收入',
subscribe_contains_ext_income         decimal(19,4)   comment '认购(不含退)-含外联佣金',
subscribe_contains_cancel_ext_amt     decimal(19,4)   comment '认购-含退、含外联GMV',
subscribe_contains_ext_amt            decimal(19,4)   comment '认购(不含退)-含外联GMV',
sign_num                              int             comment '签约量:含退、含外联',
sign_income_orig                      decimal(19,4)   comment '原合同预测总收入',
etl_time                              string          comment '任务执行时间：yyyy-MM-dd HH:mm:ss'
) comment 'TOPIC-DWS-订单业务指标表' 
partitioned by (pdate string comment '业务发生日期')
stored as parquet 
;
