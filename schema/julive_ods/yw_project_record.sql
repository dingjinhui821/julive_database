drop table if exists ods.yw_project_record;
create external table ods.yw_project_record(
id                                                     bigint          comment '主键id',
project_id                                             bigint          comment '楼盘id',
employee_id                                            bigint          comment '报备负责员工id',
project_partner_id                                     bigint          comment '合作方id',
partner_man                                            string          comment '报备对象姓名',
partner_mobile                                         string          comment '报备对象手机号',
update_datetime                                        int             comment '更新时间',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '更新者',
creator                                                bigint          comment '创建者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_project_record'
;
