drop table if exists ods.xpt_order_require_project;
create external table ods.xpt_order_require_project(
id                                                     int             comment '自增id',
order_id                                               int             comment '订单id',
project_id                                             int             comment '楼盘id',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_order_require_project'
;
