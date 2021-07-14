drop table if exists ods.cs_call_apply;
create external table ods.cs_call_apply(
id                                                     int             comment '',
employee_id                                            int             comment '发起人',
apply_reason                                           int             comment '申请原因',
`desc`                                                 string          comment '备注',
order_id                                               int             comment '订单id',
status                                                 int             comment '审批状态1:通过，2:不通过',
apply_employee_id                                      int             comment '审批人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cs_call_apply'
;
