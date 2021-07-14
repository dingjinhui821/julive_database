drop table if exists ods.bbs_praise;
create external table ods.bbs_praise(
id                                                     int             comment '自增id',
forum_id                                               int             comment '帖子id',
employee_id                                            int             comment '点赞员工id',
operation_datetime                                     int             comment '操作时间',
is_team_leader                                         int             comment '是否是负责人 1 是 2 否',
is_cancle                                              int             comment '是否取消 1 是 2 否',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_praise'
;
