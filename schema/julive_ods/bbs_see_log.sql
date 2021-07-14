drop table if exists ods.bbs_see_log;
create external table ods.bbs_see_log(
id                                                     bigint          comment '',
forum_id                                               int             comment '帖子id',
employee_id                                            int             comment '员工id',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_see_log'
;
