drop table if exists ods.yw_main_push_project_inventory_day;
create external table ods.yw_main_push_project_inventory_day(
id                                                     bigint          comment '',
acreage                                                double          comment '面积',
price                                                  int             comment '单价',
total_price                                            double          comment '总价',
sale_sets                                              int             comment '可售套数',
status                                                 int             comment '状态',
main_push_project_id                                   bigint          comment 'yw_main_push_project 表的 主键 id',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
city_id                                                int             comment '',
datetime                                               bigint          comment '周开始时间戳',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_main_push_project_inventory_day'
;
