-- dsp-关键词报告表 点击等于0 的数据:dsp_keyword_report_no_click
-- dsp-关键词报告表(百度):dsp_keyword_report_2019
-- dsp-关键词报告表(非百度):dsp_keyword_report_others

drop table if exists julive_fact.fact_dsp_keyword_report_dtl;
create table if not exists julive_fact.fact_dsp_keyword_report_dtl(
dsp_account_id           int                comment '市场投放账户ID',
account_name             string             comment '市场投放账户名',
plan_id                  bigint             comment '推广计划ID',
plan_name                string             comment '推广计划',
unit_id                  bigint             comment '推广单元ID',
unit_name                string             comment '推广单元',
channel_id               int                comment '渠道ID',
keyword_id               bigint             comment '关键词ID',
keyword_name             string             comment '关键词',

show_num                 int                comment '暂时次数',
click_num                int                comment '点击次数',
bill_cost                double             comment '账面消费', -- 账面消费
cost                     double             comment '现金消费', -- 真实消费 = 账面消费bill_cost/（1+返点）
click_rate               double             comment '点击率',
average_ranking          double             comment '平均排名',
match_type               int                comment '匹配模式(取值范围：1 – 精确匹配2 – 高级短语匹配3 – 广泛匹配)',
price                    double             comment '出价',
average_click_price      double             comment '平均点击价格',
device                   int                comment '设备类型（0：全部；1：计算机；2：移动设备）',
url                      string             comment 'URL',
clue_num                 int                comment '线索量',
clue_cost                double             comment '线索成本',
distribute_amount        int                comment '上户量',
distribute_cost          double             comment '上户',
distribute_rate          double             comment '上户率',
channel_put              string             comment '渠道投放',
status                   int                comment '数据状态（1：从API同步完成；2：关键词匹配完成；3：关联分析完成）',
creator                  bigint             comment '创建人',
updator                  bigint             comment '更新人',
create_datetime          string             comment '创建时间',
update_datetime          string             comment '更新时间',
report_date              string             comment '快照日期',
etl_time                 string             comment 'etl跑数时间'
) comment 'dsp-关键词报告明细事实表'
partitioned by(pdate string comment '分区日期')
stored as parquet
;
