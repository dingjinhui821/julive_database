drop table if exists ods.cj_project_building_info;
create external table ods.cj_project_building_info(
id                                                     bigint          comment '',
project_id                                             int             comment '楼盘id',
building_id                                            int             comment '楼栋id',
live_date_type                                         int             comment '交房时间类型 1预计交房时间 2实际交房时间',
live_date_year                                         int             comment '交房时间年份',
live_date_month                                        int             comment '交房时间月份',
live_date_day                                          int             comment '交房时间日',
live_date_ten                                          int             comment '交房时间旬 1上旬 2中旬 3下旬',
live_date                                              int             comment '交房时间戳',
live_note                                              string          comment '交房备注',
msg_send_datetime                                      int             comment '消息发送对比时间，交房时间或预计交房时间转化得来',
property_year                                          int             comment '产权年限',
property_year_other                                    string          comment '产权年限(其他)',
construction_stage                                     int             comment '建设阶段 1现房 2期房',
property_company_id                                    int             comment '物业公司id',
property_fee                                           string          comment '物业费',
decorate_info                                          int             comment '装修情况 1带装修 2毛坯',
decorate_unit                                          string          comment '装修公司',
decorate_fee                                           int             comment '装修费用',
water_electy                                           string          comment '水电气',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
business_layout                                        int             comment '建筑类型',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_building_info'
;
