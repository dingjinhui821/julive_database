drop table if exists ods.yw_subscribe_timeout;
create external table ods.yw_subscribe_timeout(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
subscribe_id                                           bigint          comment '认购表id',
type                                                   int             comment '延迟录入类型:1正式认购，3排号',
input_datetime                                         int             comment '录入时间',
real_datetime                                          int             comment '实际发生时间:订房时间，正式签认购书时间',
timeout_seconds                                        int             comment '延迟录入时间，单位秒',
create_datetime                                        int             comment '创建时间',
employee_id                                            bigint          comment '所属咨询师id',
is_cancel                                              int             comment '是否取消超时:0不取消，1取消',
employee_leader_id                                     bigint          comment '咨询师主管id',
employee_leader_realname                               string          comment '咨询师主管姓名',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_subscribe_timeout'
;
