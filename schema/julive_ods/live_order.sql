drop table if exists ods.live_order;
create external table ods.live_order(
id                                                     int             comment 'id',
order_id                                               bigint          comment '订单id',
room_id                                                int             comment '房间id',
employee_id                                            int             comment '咨询师id',
master_employee_id                                     int             comment '主持人（咨询师id）',
user_id                                                int             comment '用户id',
city_id                                                int             comment '城市id',
product_id                                             int             comment '101 android 201 ios',
project_id                                             int             comment '楼盘id',
status                                                 int             comment '状态 1待分配 2待接听 3进行中 4结束',
operation_type                                         int             comment '操作类型 1咨询师未接通 2客户未接通 3咨询师拒绝 4客户拒绝 5.无人可接单 6 咨询师已接通',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
is_valid                                               int             comment '房间是否接通（有效）1-有效',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/live_order'
;
