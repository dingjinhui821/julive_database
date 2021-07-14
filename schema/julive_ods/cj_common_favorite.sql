drop table if exists ods.cj_common_favorite;
create external table ods.cj_common_favorite(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
user_id                                                bigint          comment '用户id',
object_id                                              bigint          comment '对象id',
status                                                 int             comment '状态，1: 有效 2:假删除',
type                                                   int             comment '0:楼盘,1:楼市头条,3:户型',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_common_favorite'
;
