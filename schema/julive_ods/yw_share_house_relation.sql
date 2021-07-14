drop table if exists ods.yw_share_house_relation;
create external table ods.yw_share_house_relation(
id                                                     int             comment '',
share_id                                               int             comment '单楼盘分享表id',
project_id                                             bigint          comment '楼盘id',
star_level                                             double          comment '推荐星级',
project_summary                                        string          comment '楼盘概述',
house_summary                                          string          comment '户型概述',
house_id                                               int             comment '户型表id',
house_type                                             int             comment '户型类型（1.系统 2.自定义）',
drive_time                                             int             comment '自驾时间,单位分钟',
by_bus_time                                            int             comment '乘坐公交时间,单位分钟',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_house_relation'
;
