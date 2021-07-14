-- dsp_unit(dsp-单元信息表)清洗SQL 

drop table if exists julive_fact.fact_dsp_unit_dtl;
create table if not exists julive_fact.fact_dsp_unit_dtl(
account_id                 int             comment '账户id',
account                    string          comment '账户',
plan_id                    bigint          comment '计划ID',
plan_name                  string          comment '计划名',
unit_id                    bigint          comment '单元id',
unit_name                  string          comment '单元名',
media_type                 int             comment '媒体类型',
product_type               int             comment '媒体形态',
max_price                  double          comment '最高出价',
pause                      int             comment '是否暂停',
unit_status                int             comment '状态 31-有效 32-暂停推广 33-推广计划暂停推广',
price_ratio                double          comment '无线出价比例',
pc_price_ratio             double          comment '计算机出价比例',
accu_price_factor          double          comment '精确出价比例',
wide_price_factor          double          comment '广泛出价比例',
word_price_factor          double          comment '短语出价比例',
segment_recommend_status   int             comment '图片素材配图开关 1 – 关闭 0 – 开启，默认开启',
match_price_status         int             comment '分匹配状态 0 开启，要求精确系数>= 短语系数>=广泛系数，且三个比例系数均不能为空 1关',
negative_words             string          comment '否定关键词',
negative_words_num         string          comment '否定关键词个数',
exact_negative_words       string          comment '精确否定关键词',
exact_negative_words_num   string          comment '精确否定关键词个数',
create_datetime            string          comment '创建时间',
update_datetime            string          comment '更新时间',
report_date                string          comment '快照日期',
etl_time                   string          comment 'ETL跑数时间'
) comment 'DSP-单元信息明细事实表'
partitioned by (pdate string comment '采集日期')
stored as parquet 
;

