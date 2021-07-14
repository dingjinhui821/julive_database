drop table if exists ods.yw_sys_number_talking_task;
create external table ods.yw_sys_number_talking_task(
id                                                     int             comment 'id',
city_id                                                int             comment '城市',
sys_number_talking_id                                  int             comment 'yw_sys_number_talking id',
order_id                                               bigint          comment '订单id',
employee_id                                            int             comment '所属咨询师id',
employee_manager_id                                    int             comment '所属咨询师主管id',
user_id                                                bigint          comment '所属咨询师id',
user_realname                                          string          comment '用户姓名',
order_status                                           int             comment '订单状态',
call_begin_time                                        int             comment '通话时间',
follow_status                                          int             comment '跟进状态 1-处理中 2-已延迟 3-正常跟进完成 4-延迟跟进完成',
follow_employee_id                                     int             comment '跟进人id',
follow_type                                            int             comment '跟进类型 1-电话 2-短信 3-见面',
follow_datetime                                        int             comment '跟进时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sys_number_talking_task'
;
