drop table if exists ods.yw_share_commute_relation;
create external table ods.yw_share_commute_relation(
id                                                     int             comment '主键id',
share_id                                               int             comment '分享id',
project_id                                             int             comment '楼盘id',
commute_lng                                            string          comment '通勤地址经度',
commute_lat                                            string          comment '通勤纬度',
commute_address                                        string          comment '通勤地址',
drive_time                                             int             comment '自驾时间,单位分钟',
by_bus_time                                            int             comment '乘坐公交时间,单位分钟',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
start_address                                          string          comment '楼盘起点地址',
start_lat                                              string          comment '楼盘起点纬度',
start_lng                                              string          comment '楼盘起点经度',
bus_transit_time                                       int             comment '公交通勤时长,单位分钟',
bus_walk_time                                          int             comment '公交步行时长,单位分钟',
is_walk_show                                           int             comment '公交步行时长是否展示 0展示 1不展示',
remark                                                 string          comment '备注',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_commute_relation'
;
