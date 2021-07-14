drop table if exists ods.yw_order_confirm_record;
create external table ods.yw_order_confirm_record(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
employee_id_old                                        int             comment '老咨询师',
employee_id_new                                        int             comment '新咨询师',
first_alloc_datetime                                   int             comment '首次分配时间',
alloc_datetime                                         int             comment '分配时间',
type                                                   int             comment '操作类型:1新建，2重新分配，3退分配，4批量修改咨询师',
is_confirm                                             int             comment '是否确认0:轮空 1:确认 2:放弃',
confirm_datetime                                       int             comment '操作时间',
cause_type                                             int             comment '放弃原因',
cause_remarks                                          string          comment '放弃备注',
alloc_policy                                           int             comment '分配策略 1:a组 2:b组',
order_alloc_type                                       int             comment '分配类型',
is_system                                              int             comment '1:系统分配 2:人工分配',
is_sms                                                 int             comment '是否给管理层发提醒 0:否 1:是',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_confirm_record'
;
