drop table if exists ods.cj_user_cance;
create external table ods.cj_user_cance(
id                                                     bigint          comment 'id',
user_id                                                bigint          comment '用户id',
cance_type                                             string          comment '注销类型按照,分割',
cance_reasion                                          string          comment '拒绝原因',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
updator                                                bigint          comment '修改者',
creator                                                bigint          comment '创建者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_user_cance'
;
