drop table if exists ods.yw_sign_timeout;
create external table ods.yw_sign_timeout(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
sign_id                                                bigint          comment '签约表id',
type                                                   int             comment '延迟录入类型:1网签，3草签',
input_datetime                                         int             comment '录入时间',
real_datetime                                          int             comment '实际发生时间:草签时间，网签时间',
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
location '/dw/ods/yw_sign_timeout'
;
