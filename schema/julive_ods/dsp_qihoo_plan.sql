drop table if exists ods.dsp_qihoo_plan;
create external table ods.dsp_qihoo_plan(
id                                                     int             comment '主键',
account                                                string          comment '账户',
media_type                                             int             comment '媒体类型',
product_type                                           int             comment '媒体形态',
account_id                                             int             comment '账户id',
plan_id                                                bigint          comment '计划id',
plan_name                                              string          comment '计划名',
budget                                                 double          comment '预算',
plan_type                                              int             comment '计划类型',
device                                                 int             comment '推广设备',
negative_words                                         string          comment '否定关键词',
exact_negative_words                                   string          comment '精确否定关键词',
region_target                                          string          comment '推广区域',
region_target_code                                     string          comment '推广区域码',
pause                                                  int             comment '是否暂停',
price_ratio                                            double          comment '无线出价比例',
pc_price_ratio                                         double          comment '计算机出价比例',
bid_prefer                                             int             comment '出价优先 1:计算机优先 2:移动优先',
plan_status                                            int             comment '推广计划状态 21-有效 22-处于暂停时段 23-暂停推广 24-推广计划预算不足 25-账户预算不足',
show_prob                                              int             comment '创意展现方式 1 - 优选 2 - 轮显',
snapshot_date                                          int             comment '快照日期',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_qihoo_plan'
;
