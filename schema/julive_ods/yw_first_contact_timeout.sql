drop table if exists ods.yw_first_contact_timeout;
create external table ods.yw_first_contact_timeout(
id                                                     bigint          comment '',
order_id                                               int             comment '订单id',
alloc_type                                             int             comment '分配类型 1.手动添加2.批量分配3.指定分配4.首次分配',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
employee_id                                            bigint          comment '员工id',
employee_realname                                      string          comment '员工姓名',
distribute_datetime                                    int             comment '分配时间',
hope_contact_time                                      int             comment '期望联系实际',
delay_start_datetime                                   int             comment '延迟开始时间',
delay_deadline_datetime                                int             comment '延迟截止时间',
first_contact_datetime                                 int             comment '首次联系时间',
delayed_contact_time                                   int             comment '首次联系超时时间（s）',
is_cancel                                              int             comment '是否取消超时:0不取消，1取消',
employee_leader_id                                     bigint          comment '咨询师主管id',
employee_leader_realname                               string          comment '咨询师主管姓名',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
delay_type                                             int             comment '延迟类型:1:联系客户超时 2:系统录入超时 3:批量分配导致延迟',
delay_cause                                            int             comment '延迟原因:1:未及时查看系统、2:忘记关上户、3:开会、4:未及时录入系统、5:忘记录入系统、6:带看途中没有录入、7:指定分配造成延迟、8:指定分配后未及时联系、9:其他',
cancel_cause                                           int             comment '取消原因:1:系统自动取消（此选项不在页面展示），2:连续接到上户、3:与客户通话超过30分钟、4:手机没有信号、5:系统bug、6:其他',
delay_call_start_datetime                              int             comment '电话/短信延迟开始时间',
delay_call_deadline_datetime                           int             comment '电话/短信延迟截止时间',
delayed_call_time                                      int             comment '电话/短信延迟超时时间',
call_duration                                          int             comment '通话时长，秒',
call_contact_datetime                                  int             comment '通话/短信 联系时间',
rule_type                                              int             comment '延迟规则说明，consultantexecutiveconstant中$rule_type_desc',
cancel_datetime                                        int             comment '取消时间',
cancelor                                               int             comment '取消人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_first_contact_timeout'
;
