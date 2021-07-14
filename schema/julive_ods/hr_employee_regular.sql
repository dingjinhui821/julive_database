drop table if exists ods.hr_employee_regular;
create external table ods.hr_employee_regular(
id                                                     int             comment '',
employee_id                                            int             comment '员工id',
regular_datetime                                       bigint          comment '转正时间',
remark                                                 string          comment '转正评价',
attachment                                             string          comment '附件地址',
origin_name                                            string          comment '附件名称',
create_datetime                                        bigint          comment '',
update_datetime                                        bigint          comment '',
creator                                                int             comment '',
updator                                                int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hr_employee_regular'
;
