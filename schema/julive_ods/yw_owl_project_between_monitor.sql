drop table if exists ods.yw_owl_project_between_monitor;
create external table ods.yw_owl_project_between_monitor(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
see_project_id                                         bigint          comment '带看id',
see_employee_id                                        bigint          comment '带看咨询师id',
city_id                                                bigint          comment '城市id',
city_name                                              string          comment '城市名称',
taxi_order_id                                          bigint          comment '打车单id',
taxi_channel_type                                      int             comment '打车渠道:1 滴滴 2 易道 3曹操',
stime                                                  int             comment '楼盘间路程开始时间',
etime                                                  int             comment '楼盘间路程结束时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_owl_project_between_monitor'
;
