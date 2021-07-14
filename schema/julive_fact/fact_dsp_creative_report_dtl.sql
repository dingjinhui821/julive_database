-- dsp-创意报告表：dsp_creative_report

drop table if exists julive_fact.fact_dsp_creative_report_dtl;
create table if not exists julive_fact.fact_dsp_creative_report_dtl(
id                                                bigint            COMMENT 'id',
dsp_account_id                                    int               comment '市场投放账户id',
account_name                                      string            comment '市场投放账户名',
pic_num                                           int               comment '图片数量',
plan_name                                         string            comment '推广计划',
plan_id                                           bigint            comment '推广计划id',
unit_name                                         string            comment '推广单元',
unit_id                                           bigint            comment '推广单元id',
title                                             string            comment '创意标题',
creative_name                                     string            comment '创意名称',
creative_id                                       bigint            comment '创意id',
show_num                                          int               comment '展示次数',
click_num                                         int               comment '点击次数',
cost                                              double            comment '现金消费',
bill_cost                                         double            comment '账面消费',
click_rate                                        double            comment '点击率',
price                                             double            comment '出价',
average_click_price                               double            comment '平均点击价格',
bid_type                                          int               comment '付费模式 0未识别 1点击 3转化',
device                                            int               comment '设备类型（0：全部；1：计算机；2：移动设备）',
url                                               string            comment 'url',
channel_id                                        int               comment '渠道id',
channel_put                                       string            comment '渠道投放',
clue_num                                          int               comment '线索量',
clue_cost                                         double            comment '线索成本',
distribute_amount                                 int               comment '上户量',
distribute_cost                                   double            comment '上户',
distribute_rate                                   double            comment '上户率',
report_date                                       string            comment '日期',
create_time                                       string            comment '创建时间',
update_time                                       string            comment '更新时间',
creator                                           bigint            comment '创建人',
updator                                           bigint            comment '更新人',
status                                            int               comment '数据状态（1：从api同步完成；2：关键词匹配完成；3：关联分析完成）',
adgroup_id                                        bigint            COMMENT '广告组id 微信广告主专用',
etl_time                                          string            comment 'ETL跑数时间'
) comment 'dsp-创意报告明细事实表'
stored as parquet
;


