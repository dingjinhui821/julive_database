drop table if exists ods.bd_building_info;
create external table ods.bd_building_info(
id                                                     int             comment '',
building_name                                          string          comment '楼栋名称',
project_id                                             bigint          comment '楼盘id',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '更新时间',
cj_building_id                                         bigint          comment '关联居理楼栋id',
source_building_id                                     bigint          comment '源楼栋id',
relation_source_houses_id                              string          comment '关联源户型id,多个(,)号分隔',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
relation_house_is_change                               int             comment '关联户型是否有变化 1有 2无',
parent_building_id                                     int             comment '父楼栋id',
original_house_num                                     string          comment '原始户型编号',
saling_status                                          int             comment '销售状态',
project_type_flag                                      int             comment '楼盘业态对比标记 0 不确定 1正常 2不属于该楼盘',
above_floor_num                                        int             comment '地上楼层数',
unit_num                                               int             comment '单元数',
ladder_ratio                                           string          comment '梯户比',
total_house_num                                        int             comment '总户数',
building_type                                          int             comment '建筑类型',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_building_info'
;
