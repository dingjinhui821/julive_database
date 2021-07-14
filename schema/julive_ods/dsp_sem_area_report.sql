drop table if exists ods.dsp_sem_area_report;
create external table ods.dsp_sem_area_report(
id                                                     int             comment 'id',
dsp_account_id                                         int             comment '市场投放账户id',
account_name                                           string          comment '市场投放账户名',
media_type                                             int             comment '媒体类型',
product_type                                           int             comment '产品形态',
plan_name                                              string          comment '推广计划',
plan_id                                                bigint          comment '推广计划id',
show_num                                               int             comment '暂时次数',
click_num                                              int             comment '点击次数',
bill_cost                                              double          comment '账面消费',
cost                                                   double          comment '现金消费',
click_rate                                             double          comment '点击率',
cpm                                                    double          comment '千次展示消费',
average_ranking                                        int             comment '平均排名',
average_click_price                                    double          comment '平均点击价格',
city                                                   string          comment '城市',
city_code                                              int             comment '城市区域码',
province                                               string          comment '省',
province_code                                          int             comment '省区域码',
device                                                 int             comment '设备类型（0:全部1:计算机2:移动设备）',
report_date                                            int             comment '日期',
report_level                                           int             comment '报告维度 1 账户 2 计划',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_sem_area_report'
;
