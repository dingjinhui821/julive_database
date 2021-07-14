drop table if exists ods.ex_invitation;
create external table ods.ex_invitation(
id                                                     int             comment '',
ex_order_id                                            int             comment 'bd单id',
follow_employee_id                                     int             comment '跟进bd人',
plan_datetime                                          int             comment '预约模拟邀约时间',
real_datetime                                          int             comment '实际模拟邀约时间',
ex_order_status                                        int             comment 'bd单状态',
status                                                 int             comment '状态 1预约 2实际',
result                                                 int             comment '模拟邀约结果 1通过 2未通过',
manager_suggest                                        string          comment '主管建议',
manager_confirm                                        int             comment '主管确认结果 1通过 2未通过',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_invitation'
;
