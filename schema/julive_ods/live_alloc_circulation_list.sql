drop table if exists ods.live_alloc_circulation_list;
create external table ods.live_alloc_circulation_list(
id                                                     int             comment '主键id',
employee_id                                            int             comment '咨询师id',
room_id                                                int             comment '房间id',
alloc_type                                             int             comment '分配类型（1.指定分配 2.正常分配）',
circulation_type                                       int             comment '流转类型 1.分配 2.第一次流转 3.第二次流转 4.第三次流转',
operation_type                                         int             comment '操作:0.正常 1.咨询师拒接 2.咨询师忙碌',
circulation_from                                       int             comment '要求流转来源 1.console 2.app',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/live_alloc_circulation_list'
;
