drop table if exists ods.yw_share_apartment_relation;
create external table ods.yw_share_apartment_relation(
id                                                     int             comment '主键id',
share_id                                               int             comment '分享id',
project_id                                             int             comment '楼id',
apartment_id                                           int             comment '户型id',
apartment_type                                         int             comment '户型类型（1.系统 2:自定义）',
apartment_name                                         string          comment '户型名称',
square_metre                                           string          comment '平米数',
total_price                                            double          comment '总价',
total_price_max                                        double          comment '总价最大值',
apartment_pic                                          string          comment '户型图',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
order_asc                                              int             comment '排序越小越靠前',
house_status                                           int             comment '状态，1: 未售  2:在售  3:售罄 4:待售 5: 排卡中',
house_summary                                          string          comment '户型概述',
star_level                                             double          comment '推荐星级',
room_type                                              int             comment '厅室类别:0不限, 1一居, 2二居, 3三居, 4四居, 5五居及以上, 6 loft,7 开间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_apartment_relation'
;
