drop table if exists ods.cj_project_notify_info;
create external table ods.cj_project_notify_info(
id                                                     int             comment '主键id',
notify_id                                              int             comment '变更通知id',
project_id                                             int             comment '楼盘id',
sale_status                                            int             comment '销售状态',
price_type                                             int             comment '价格类型',
min_price                                              int             comment '价格最小值',
max_price                                              int             comment '价格最大值',
total_price_type                                       int             comment '总价类型',
min_total_price                                        int             comment '总价最小值',
max_total_price                                        int             comment '总价最大值',
discount                                               string          comment '折扣描述',
data_from                                              int             comment '数据来源角色 1.拓展录入、2.商务录入、3.驻场录入、4.运营市调录入、5.咨询师录入、6.竞对系统录入、7.运营-商务录入，10.其他',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_notify_info'
;
