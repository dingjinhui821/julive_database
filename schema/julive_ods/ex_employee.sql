drop table if exists ods.ex_employee;
create external table ods.ex_employee(
id                                                     int             comment '',
employee_id                                            int             comment '拓展员工id',
skip_invite                                            int             comment '邀约是否免检 1是 2否',
skip_talk                                              int             comment '谈判是否免检 1是 2否',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_employee'
;
