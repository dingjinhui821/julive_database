drop table if exists ods.yw_share_traffic_relation;
create external table ods.yw_share_traffic_relation(
id                                                     int             comment '主键id',
share_id                                               int             comment '分享id',
project_id                                             int             comment '楼盘id',
traffic_id                                             int             comment '交通id',
traffic_type                                           int             comment '交通类型(1.系统(百度查的) 2:自定义)',
traffic_name                                           string          comment '交通名称',
traffic_category                                       int             comment '交通分类(1公交. 2:地铁)',
station_name                                           string          comment '站点名称',
distance                                               int             comment '距离(米)',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_traffic_relation'
;
