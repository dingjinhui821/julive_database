drop table if exists ods.yw_order_pause;
create external table ods.yw_order_pause(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '员工id',
pause_cause                                            int             comment '申请暂停原因:',
plan_time                                              int             comment '联系计划时间',
contact_employee                                       bigint          comment '联系人',
contact_plan                                           string          comment '联系计划',
reset_up_time                                          int             comment '重启时间',
audit_status                                           int             comment '主管审核状态:0 待审核 1 审核通过 2 驳回',
audit_time                                             int             comment '审核时间',
audit_employee                                         bigint          comment '审核人',
reset_up_type                                          int             comment '重启类型:1系统自动重启2手动重启',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_pause'
;
