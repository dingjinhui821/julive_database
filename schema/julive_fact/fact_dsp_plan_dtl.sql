-- dsp_plan(dsp-计划信息表)清洗SQL 

drop table if exists julive_fact.fact_dsp_plan_dtl;
create table if not exists julive_fact.fact_dsp_plan_dtl(
account_id                    int             comment '账户id',
account                       string          comment '账户',
plan_id                       bigint          comment '计划ID',
plan_name                     string          comment '计划名',
plan_type                     int             comment '计划类型',
media_type                    int             comment '媒体类型',
product_type                  int             comment '媒体形态',
budget                        double          comment '预算',
device                        int             comment '推广设备',
region_target                 string          comment '推广区域',
region_target_code            string          comment '推广区域码',
pause                         int             comment '是否暂停',
price_ratio                   double          comment '无线出价比例',
pc_price_ratio                double          comment '计算机出价比例',
bid_prefer                    int             comment '出价优先 1：计算机优先 2：移动优先',
plan_status                   int             comment '推广计划状态 21-有效 22-处于暂停时段 23-暂停推广 24-推广计划预算不足 25-账户预算不足',
show_prob                     int             comment '创意展现方式 1 - 优选 2 - 轮显',
negative_words                string          comment '否定关键词',
negative_words_num            string          comment '否定关键词个数',
exact_negative_words          string          comment '精确否定关键词',
exact_negative_words_num      string          comment '精确否定关键词个数',
create_datetime               string          comment '创建时间',
update_datetime               string          comment '更新时间',
report_date                   string          comment '采集日期',
etl_time                      string          comment 'ETL跑数时间'
) comment 'DSP-计划信息明细事实表' 
partitioned by (pdate string comment '采集日期')
stored as parquet 
;
