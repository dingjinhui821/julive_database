drop table if exists ods.dsp_keyword_report_no_click;
create external table ods.dsp_keyword_report_no_click(
id                                                     bigint          comment 'id',
dsp_account_id                                         int             comment '市场投放账户id',
account_name                                           string          comment '市场投放账户名',
plan_name                                              string          comment '推广计划',
plan_id                                                bigint          comment '推广计划id',
unit_name                                              string          comment '推广单元',
unit_id                                                bigint          comment '推广单元id',
keyword_name                                           string          comment '关键词',
keyword_id                                             bigint          comment '关键词id',
show_num                                               int             comment '暂时次数',
click_num                                              int             comment '点击次数',
bill_cost                                              double          comment '账面消费',
cost                                                   double          comment '现金消费',
click_rate                                             double          comment '点击率',
average_ranking                                        double          comment '平均排名',
match_type                                             int             comment '匹配模式(取值范围:1 – 精确匹配2 – 高级短语匹配3 – 广泛匹配)',
price                                                  double          comment '出价',
average_click_price                                    double          comment '平均点击价格',
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
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_keyword_report_no_click'
;
