drop table if exists ods.yw_user_project_browse_history;
create external table ods.yw_user_project_browse_history(
id                                                     bigint          comment '',
user_id                                                int             comment '用户id',
project_id                                             string          comment '浏览过的楼盘id',
user_ip                                                string          comment '用户ip',
browse_type                                            int             comment '浏览类型 1pc 2.elite 3.m',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '记录更新时间',
is_split_project_id                                    int             comment '是否将字符串的project_id拆分为单个id，默认为:0:否1:已拆分',
single_project_id                                      bigint          comment '拆分后的单个楼盘id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_user_project_browse_history'
;
