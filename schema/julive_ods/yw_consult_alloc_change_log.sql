drop table if exists ods.yw_consult_alloc_change_log;
create external table ods.yw_consult_alloc_change_log(
id                                                     bigint          comment '',
employee_id                                            bigint          comment '咨询师id',
employee_name                                          string          comment '咨询师姓名',
can_alloc                                              int             comment '咨询师自己控制是否可以分配:1可以，0不可以',
create_datetime                                        bigint          comment '',
update_datetime                                        bigint          comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_consult_alloc_change_log'
;
