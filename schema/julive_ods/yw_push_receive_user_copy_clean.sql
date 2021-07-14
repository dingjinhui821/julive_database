drop table if exists julive_bak.yw_push_receive_user_copy_clean;
create external table julive_bak.yw_push_receive_user_copy_clean(
id                                                     bigint          comment '自增id',
app_id                                                 int             comment '居理生成的app_id',
unique_id                                              string          comment '设备id',
batch                                                  string          comment '批次',
title                                                  string          comment '标题',
notification                                           string          comment '推送内容',
send_time                                              int             comment '发送时间',
push_params                                            string          comment '推送参数',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_push_receive_user_copy_clean'
;
