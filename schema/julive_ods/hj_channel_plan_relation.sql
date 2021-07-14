drop table if exists ods.hj_channel_plan_relation;
create external table ods.hj_channel_plan_relation(
id                                                     int             comment '主键id',
channel_id                                             int             comment '渠道id',
channel_name                                           string          comment '渠道名称',
media_type                                             int             comment '媒体类型 1:百度,2:360,3:搜狗,11:神马',
product_type                                           int             comment '产品形态 4:sem',
account_id                                             int             comment '账户id',
account                                                string          comment '账户名称',
plan_id                                                bigint          comment '计划id,0全部计划',
plan_name                                              string          comment '计划名称',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_channel_plan_relation'
;
