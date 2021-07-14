drop table if exists ods.yw_server_alloc;
create external table ods.yw_server_alloc(
employee_id                                            bigint          comment '客服id',
update_datetime                                        int             comment '更新时间',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '更新者',
creator                                                bigint          comment '创建者',
can_alloc                                              int             comment '客服自己控制是否可以分配:1可以，0不可以',
status                                                 int             comment '管理者配置客服状态:2还不能接待客户，1能够接待客户',
init_weight                                            int             comment '新客服分配用户的起步权重:100为标准平均值，小于100低于平均值，大于100高于平均值',
init_alloc_num                                         int             comment '新客服分配用户的起步计算数目，不能人为设置，由系统计算:值越大分配到客户的几率越小',
real_alloc_num                                         int             comment '客服当前实际已经分配的用户数',
rank_alloc_num                                         int             comment '用来排序的咨客服分配用户数',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
last_can_alloc_datetime                                bigint          comment '最近一次打开可以接单的时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_server_alloc'
;
