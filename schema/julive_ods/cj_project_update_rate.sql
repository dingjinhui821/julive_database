drop table if exists ods.cj_project_update_rate;
create external table ods.cj_project_update_rate(
id                                                     int             comment '',
project_id                                             int             comment '楼盘id',
update_rate                                            int             comment '本周更新度',
business_update_rate                                   int             comment '本周商务更新度',
week                                                   int             comment '周',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_update_rate'
;
