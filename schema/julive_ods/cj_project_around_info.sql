drop table if exists ods.cj_project_around_info;
create external table ods.cj_project_around_info(
id                                                     bigint          comment '主键',
city_id                                                int             comment '城市id',
project_id                                             bigint          comment '楼盘id',
type                                                   int             comment '类型',
name                                                   string          comment '名称',
distance                                               int             comment '直线距离',
address                                                string          comment '地址',
walk_distance                                          int             comment '步行距离',
walk_duration                                          int             comment '步行时间',
tag                                                    string          comment '标签',
api_type                                               string          comment 'api类型',
lng                                                    string          comment '经度',
lat                                                    string          comment '纬度',
score                                                  int             comment '得分',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_around_info'
;
