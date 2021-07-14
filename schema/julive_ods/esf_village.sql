drop table if exists ods.esf_village;
create external table ods.esf_village(
id                                                     int             comment '',
source                                                 int             comment '来源',
village_spider_id                                      int             comment '抓取小区id',
village_source_id                                      int             comment '抓取源网站，小区id',
city_id                                                int             comment '城市id',
name                                                   string          comment '小区名',
url                                                    string          comment '小区详情页地址',
url_deal                                               string          comment '小区成交列表页地址',
address                                                string          comment '地址',
district                                               string          comment '区域',
district_id                                            int             comment '区域id',
trade_area                                             string          comment '商圈',
trade_area_id                                          int             comment '商圈id',
building_year                                          string          comment '建筑年代',
building_type                                          string          comment '建筑类型',
building_num                                           string          comment '栋数',
house_num                                              string          comment '户数',
averge_price                                           string          comment '均价',
coodinate                                              string          comment '坐标',
house_volume                                           int             comment '在售房源量',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
village_status                                         int             comment '状态:1正常，2删除',
has_property                                           int             comment '是否导入业主资料 1:是，2:否',
principal                                              string          comment '负责人',
is_star                                                int             comment '是否明星小区1:是，2:否',
star_room                                              string          comment '明星户型',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_village'
;
