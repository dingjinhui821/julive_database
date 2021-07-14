drop table if exists ods.yw_share_project_relation;
create external table ods.yw_share_project_relation(
id                                                     int             comment '主键id',
share_id                                               int             comment '分享id',
project_id                                             int             comment '楼盘id',
city_id                                                bigint          comment '城市id',
project_status                                         int             comment '销售状态，1: 未售 2:在售 3:售罄 4:待售',
price_min                                              double          comment '最小总价',
price_max                                              double          comment '最大总价',
unit_price                                             double          comment '单价',
project_type                                           bigint          comment '业态',
decorate                                               string          comment '装修情况,1:带装修 2:毛坯(多选)',
manage_fee                                             string          comment '物业费',
voice_is_show                                          int             comment '楼盘语音是否展示 0展示 1不展示',
project_summary                                        string          comment '楼盘概述',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_project_relation'
;
