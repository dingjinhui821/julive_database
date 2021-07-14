drop table if exists ods.xpt_house_attr;
create external table ods.xpt_house_attr(
id                                                     int             comment '主键id',
house_id                                               int             comment '房源id',
sell_point                                             string          comment '核心卖点',
house_desc                                             string          comment '户型介绍',
near_matching                                          string          comment '周边配套',
tax_analysis                                           string          comment '税费分析',
service_desc                                           string          comment '服务介绍',
traffic                                                string          comment '交通出行',
near_subway_list                                       string          comment '地铁线列表 多个逗号分隔',
owner_mentality                                        string          comment '业主心态',
decoration_desc                                        string          comment '装修描述',
school_quota                                           string          comment '学区名额',
decoration_status                                      int             comment '装修情况1.毛坯 2.简装 3.精装',
decoration_year                                        int             comment '装修日期(年)',
decoration_month                                       int             comment '装修日期(月)',
decoration_day                                         int             comment '装修日期(日)',
is_parking                                             int             comment '有无车位 1有 2无',
living_year                                            int             comment '交房时间(年)',
living_month                                           int             comment '交房时间(月)',
living_day                                             int             comment '交房时间(日)',
house_pattern                                          int             comment '房屋格局',
presentation                                           string          comment '赠送情况',
is_household                                           int             comment '有无户口 1有 2无',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_house_attr'
;
