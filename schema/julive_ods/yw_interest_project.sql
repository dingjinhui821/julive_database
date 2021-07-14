drop table if exists ods.yw_interest_project;
create external table ods.yw_interest_project(
id                                                     int             comment '',
order_id                                               int             comment '订单id',
project_id                                             int             comment '感兴趣楼盘id',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_interest_project'
;
