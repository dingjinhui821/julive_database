drop table if exists ods.xpt_village;
create external table ods.xpt_village(
id                                                     bigint          comment '自增id',
city_id                                                int             comment '城市id',
name                                                   string          comment '小区名',
address                                                string          comment '地址',
lng                                                    string          comment '小区经度',
lat                                                    string          comment '小区纬度',
coordinate                                             string          comment '百度坐标,以空格分割  （经度  纬度），方便opensearch 按距离查询',
district_id                                            int             comment '区域id',
trade_area_id                                          int             comment '商圈id',
index_url                                              string          comment '首图url',
building_year                                          string          comment '建筑年代',
building_num                                           int             comment '栋数',
house_num                                              int             comment '户数',
averge_price                                           double          comment '均价',
sequential                                             double          comment '环比',
covers_area                                            int             comment '占地面积',
`loop`                                                 string          comment '环线',
overall_floorage                                       int             comment '总建筑面积',
is_hot                                                 int             comment '是否是热门小区 1 是 2 否',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
village_status                                         int             comment '状态:1正常，2删除',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_village'
;
