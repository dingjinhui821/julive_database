drop table if exists ods.cj_collect_user_app;
create external table ods.cj_collect_user_app(
id                                                     bigint          comment '',
create_datetime                                        int             comment '创建时间',
app_id                                                 int             comment 'appid',
user_id                                                int             comment '用户id',
install_id                                             int             comment '设备id',
unique_id                                              string          comment '设备唯一标识',
app_list                                               string          comment 'app列表(json_encode存储)',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_collect_user_app'
;
