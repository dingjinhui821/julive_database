drop table if exists ods.cj_building_info_history;
create external table ods.cj_building_info_history(
id                                                     bigint          comment '',
cj_building_info_id                                    int             comment '楼栋表id',
building_name                                          string          comment '楼栋名称',
saling_status                                          int             comment '销售状态',
batch                                                  string          comment '期数',
creator                                                bigint          comment '',
create_datetime                                        int             comment '',
updator                                                bigint          comment '',
update_datetime                                        int             comment '',
project_id                                             int             comment '楼盘id',
coordinate                                             string          comment '坐标',
building_type                                          int             comment '楼栋类型',
unit_num                                               int             comment '单元数',
total_house_num                                        int             comment '总户数',
is_relation_house_type                                 int             comment '是否关联户型 1是 2否',
is_have_building_img                                   int             comment '是否有楼栋图 1是 2否',
is_show                                                int             comment '是否显示 1显示 2不显示',
is_delete                                              int             comment '是否删除 1.是 2.否',
used_name                                              string          comment '曾用名',
above_floor_type                                       int             comment '地上楼层类型 1最高 2普通',
above_floor_num                                        int             comment '地上楼层数',
under_floor_num                                        int             comment '地下楼层数',
ladder_ratio                                           string          comment '梯户比',
on_sale_num                                            int             comment '在售房源数量',
contruct_type                                          int             comment '建筑类型(已废弃)',
storey_height                                          string          comment '层高',
relation_license_buildings                             string          comment '关联的证件楼栋id，逗号分隔',
relation_house_types                                   string          comment '关联户型id，逗号分隔',
remark                                                 string          comment '楼栋备注',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_building_info_history'
;
