drop table if exists ods.cj_project_map_info;
create external table ods.cj_project_map_info(
id                                                     bigint          comment '主键',
city_id                                                int             comment '城市id',
project_id                                             bigint          comment '楼盘id',
julive_type_one                                        int             comment '居理对应的一级分类',
julive_type_two                                        int             comment '居理对应的二级分类',
map_type_one                                           int             comment '百度对应的一级分类',
map_type_two                                           int             comment '百度对应的二级分类',
name                                                   string          comment '名称',
distance                                               int             comment '距离该楼盘的直线距离',
address                                                string          comment '地址',
walk_distance                                          int             comment '距离该楼盘的步行距离',
walk_duration                                          int             comment '步行所需时间',
tag                                                    string          comment '标签',
api_type                                               string          comment '百度地图api返回的类型',
area                                                   string          comment '区域',
lat                                                    string          comment '纬度',
lng                                                    string          comment '经度',
project_lat                                            string          comment '楼盘纬度',
project_lng                                            string          comment '楼盘经度',
is_com_walk                                            int             comment '是否计算完步行距离，是1，否2',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_map_info'
;
