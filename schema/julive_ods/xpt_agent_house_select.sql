drop table if exists ods.xpt_agent_house_select;
create external table ods.xpt_agent_house_select(
id                                                     int             comment '自增id',
business_id                                            bigint          comment '楼盘id',
agent_id                                               bigint          comment '经纪人id',
business_type                                          int             comment '数据类型 2新房 注:该表目前只存新房数据',
is_delete                                              int             comment '是否删除 1.是， 2.否',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '修改时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_agent_house_select'
;
