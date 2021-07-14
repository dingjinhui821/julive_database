drop table if exists ods.yw_employee_ab_group;
create external table ods.yw_employee_ab_group(
id                                                     int             comment '',
employee_id                                            bigint          comment '咨询师id',
type                                                   int             comment '分组类型:2北斗计划',
city_id                                                bigint          comment '城市id',
grouping                                               int             comment '咨询师所在组:1a组2b组',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_ab_group'
;
