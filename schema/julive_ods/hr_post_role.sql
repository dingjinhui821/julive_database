drop table if exists ods.hr_post_role;
create external table ods.hr_post_role(
id                                                     int             comment 'id',
post_id                                                int             comment '岗位id',
role_name                                              string          comment '角色名称',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_post_role'
;
