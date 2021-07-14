drop table if exists ods.hj_jrtt_creative_to_plan;
create external table ods.hj_jrtt_creative_to_plan(
id                                                     int             comment 'id',
account_id                                             bigint          comment '账户id',
ad_group_id                                            bigint          comment '广告组id',
plan_id                                                bigint          comment '计划id',
creative_id                                            int             comment '创意id',
request_status                                         int             comment '状态:0未请求1:成功2:失败',
creative_fail_reasion                                  string          comment '创意失败原因',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
toutiao_creative_id                                    int             comment '头条的创意id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_jrtt_creative_to_plan'
;
