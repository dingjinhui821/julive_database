drop table if exists ods.cj_house_type_notify_info;
create external table ods.cj_house_type_notify_info(
id                                                     int             comment '主键id',
notify_id                                              int             comment '变动通知id',
project_id                                             int             comment '楼盘id',
house_type_id                                          int             comment '户型id',
house_type_num                                         string          comment '户型编号',
is_area                                                int             comment '是否为面积段 1是 2否',
min_area                                               double          comment '最小面积',
max_area                                               double          comment '最大面积',
layout_ting                                            int             comment '厅',
layout_shi                                             int             comment '室',
layout_chu                                             int             comment '厨',
layout_wei                                             int             comment '卫',
room_type                                              int             comment '厅室类别:0不限, 1一居, 2二居, 3三居, 4四居, 5五居及以上, 6 loft',
status                                                 int             comment '销售状态，1: 未售 2:在售 3:售罄 4:待售',
building_type                                          int             comment '户型建筑类型:0不限,1独栋,2双拼,3联排,4叠拼,5平层,6复式,7洋房,8loft,9非loft',
acreage                                                double          comment '建筑面积',
ac_acreage                                             double          comment '套内面积',
min_offer_price                                        int             comment '最小单价',
max_offer_price                                        int             comment '最大单价',
min_total_price                                        int             comment '最小总价',
max_total_price                                        int             comment '最大总价',
house_on_sale_num                                      int             comment '在售余量',
house_on_sale_tag                                      int             comment '剩余房源标签:1房源充足,2仅剩顶底层,3仅剩顶层,4仅剩底层,5少于10套',
house_type_imgs                                        string          comment '户型图',
building_ids                                           string          comment '关联楼栋ids',
data_from                                              int             comment '数据来源角色 1.拓展录入、2.商务录入、3.驻场录入、4.运营市调录入、5.咨询师录入、6.竞对系统录入、7.运营-商务录入，10.其他',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_house_type_notify_info'
;
