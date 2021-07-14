drop table if exists ods.cj_common_user_follow;
create external table ods.cj_common_user_follow(
id                                                     int             comment '主键id',
type                                                   int             comment '1.咨询师，2.虚拟人物',
obj_id                                                 int             comment '关注的对象的id',
user_id                                                int             comment '用户id，关联cj_user表',
status                                                 int             comment '状态 1已关注 2取关注',
create_datetime                                        int             comment '提交时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_common_user_follow'
;
