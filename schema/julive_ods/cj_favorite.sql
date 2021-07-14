drop table if exists ods.cj_favorite;
create external table ods.cj_favorite(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
user_id                                                bigint          comment '用户id',
project_id                                             bigint          comment '楼盘id',
status                                                 int             comment '状态，1: 有效 2:假删除',
city_id                                                int             comment '城市id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_favorite'
;
