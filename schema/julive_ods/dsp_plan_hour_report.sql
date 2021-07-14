drop table if exists ods.dsp_plan_hour_report;
create external table ods.dsp_plan_hour_report(
id                                                     int             comment '主键id',
report_date                                            int             comment '报告日期',
report_hour                                            int             comment '小时 0-23',
media_type                                             int             comment '1:百度,2:360,3:搜狗,11:神马',
account_id                                             int             comment '账户id',
account                                                string          comment '账户名称',
plan_id                                                bigint          comment '计划id',
show_num                                               int             comment '展示',
click_num                                              int             comment '点击量',
cost                                                   double          comment '现金消费',
bill_cost                                              double          comment '账面消耗',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_plan_hour_report'
;
