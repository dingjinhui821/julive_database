drop table if exists ods.dsp_creative_report;
create external table ods.dsp_creative_report(
id                                                     bigint          comment 'id',
dsp_account_id                                         int             comment '市场投放账户id',
account_name                                           string          comment '市场投放账户名',
pic_num                                                int             comment '图片数量',
plan_name                                              string          comment '推广计划',
plan_id                                                bigint          comment '推广计划id',
unit_name                                              string          comment '推广单元',
unit_id                                                bigint          comment '推广单元id',
title                                                  string          comment '创意标题',
creative_name                                          string          comment '创意名称',
creative_id                                            bigint          comment '创意id',
show_num                                               int             comment '展示次数',
click_num                                              int             comment '点击次数',
cost                                                   double          comment '现金消费',
bill_cost                                              double          comment '账面消费',
click_rate                                             double          comment '点击率',
price                                                  double          comment '出价',
average_click_price                                    double          comment '平均点击价格',
bid_type                                               int             comment '付费模式 0未识别 1点击 3转化',
device                                                 int             comment '设备类型（0:全部1:计算机2:移动设备）',
url                                                    string          comment 'url',
channel_id                                             int             comment '渠道id',
channel_put                                            string          comment '渠道投放',
clue_num                                               int             comment '线索量',
clue_cost                                              double          comment '线索成本',
distribute_amount                                      int             comment '上户量',
distribute_cost                                        double          comment '上户',
distribute_rate                                        double          comment '上户率',
report_date                                            int             comment '日期',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
status                                                 int             comment '数据状态（1:从api同步完成2:关键词匹配完成3:关联分析完成）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_creative_report'
;
