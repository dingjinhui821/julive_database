drop table if exists ods.yw_subway_pos;
create external table ods.yw_subway_pos(
id                                                     int             comment '',
subway_list                                            string          comment '地铁线路',
subway_station                                         string          comment '地铁站',
lng                                                    string          comment '经度',
lat                                                    string          comment '纬度',
status                                                 int             comment '1:已建成，2:建设中，3:规划中',
city_id                                                int             comment '城市',
create_datetime                                        bigint          comment '',
update_datetime                                        bigint          comment '',
starting_datetime                                      string          comment '开工时间',
traffic_datetime                                       string          comment '通车时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_subway_pos'
;
