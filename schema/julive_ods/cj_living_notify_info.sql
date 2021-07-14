drop table if exists ods.cj_living_notify_info;
create external table ods.cj_living_notify_info(
id                                                     int             comment '主键id',
notify_id                                              int             comment '变动通知id',
project_id                                             int             comment '楼盘id',
live_date_type                                         int             comment '放量计划类型',
live_date_year                                         int             comment '交房时间 年',
live_date_month                                        int             comment '交房时间 月',
live_date_day                                          int             comment '交房时间 日',
live_date_ten                                          int             comment '交房时间 询',
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
location '/dw/ods/cj_living_notify_info'
;
