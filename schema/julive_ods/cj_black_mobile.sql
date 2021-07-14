drop table if exists ods.cj_black_mobile;
create external table ods.cj_black_mobile(
id                                                     int             comment '',
city_id                                                int             comment '城市id',
mobile                                                 string          comment '手机号码',
name                                                   string          comment '用户姓名',
note                                                   string          comment '备注',
creator                                                int             comment '添加人id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
status                                                 int             comment '状态，0正常，1删除',
reason                                                 int             comment '拉人黑名单原因',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_black_mobile'
;
