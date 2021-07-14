drop table if exists ods.xpt_agent_comments;
create external table ods.xpt_agent_comments(
id                                                     int             comment '自增id',
house_id                                               bigint          comment '房源id',
user_id                                                bigint          comment '经纪人id',
sell_point                                             string          comment '核心卖点',
village_introduction                                   string          comment '小区介绍',
house_type_introduction                                string          comment '户型介绍',
peripheral_matching                                    string          comment '周边配套',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_agent_comments'
;
