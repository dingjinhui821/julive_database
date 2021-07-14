drop table if exists ods.employee_track_julive;
create external table ods.employee_track_julive(
id                                                     int             comment '',
employee_id                                            int             comment '员工id',
employee_id_in_token                                   int             comment '上传数据时的咨询师id',
city_id                                                int             comment '',
location_coordinate                                    string          comment '打卡坐标',
lat                                                    string          comment '打卡位置所在纬度',
lng                                                    string          comment '打卡位置所在经度',
location_datetime                                      int             comment '打卡时间',
distance                                               double          comment '打卡的位置到公司的距离 km',
is_in                                                  int             comment '是否在公司内 1是2否',
company_lat                                            string          comment '公司位置所在纬度',
company_lng                                            string          comment '公司所在位置经度',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                int             comment '',
updator                                                int             comment '',
speed                                                  double          comment '速度，单位:km/h',
radius                                                 double          comment '定位精度，单位:m',
direction                                              int             comment '方向，范围为[0,359]，0度为正北方向，顺时针',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/employee_track_julive'
;
