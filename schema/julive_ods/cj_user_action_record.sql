drop table if exists ods.cj_user_action_record;
create external table ods.cj_user_action_record(
id                                                     bigint          comment '主键id',
create_datetime                                        int             comment '注销时间或设置推送时间',
user_id                                                bigint          comment '用户id',
nickname                                               string          comment '用户昵称(注销前用户昵称)',
realname                                               string          comment '真实姓名(注销前真实姓名)',
mobile                                                 string          comment '手机号(注销前手机号)',
email                                                  string          comment '邮箱(注销前邮箱)',
push_type                                              int             comment '1开启 2关闭',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_user_action_record'
;
