drop table if exists ods.dsp_sogou_unit;
create external table ods.dsp_sogou_unit(
id                                                     int             comment '主键',
account                                                string          comment '账户',
media_type                                             int             comment '媒体类型',
product_type                                           int             comment '媒体形态',
account_id                                             int             comment '账户id',
plan_id                                                bigint          comment '计划id',
plan_name                                              string          comment '计划名',
unit_id                                                bigint          comment '单元id',
unit_name                                              string          comment '计划名',
max_price                                              double          comment '最高出价',
pause                                                  int             comment '是否暂停',
negative_words                                         string          comment '否定关键词',
exact_negative_words                                   string          comment '精确否定关键词',
unit_status                                            int             comment '状态',
price_ratio                                            double          comment '无线出价比例',
pc_price_ratio                                         double          comment '计算机出价比例',
accu_price_factor                                      double          comment '精确出价比例',
wide_price_factor                                      double          comment '广泛出价比例',
word_price_factor                                      double          comment '短语出价比例',
segment_recommend_status                               int             comment '图片素材配图开关 1 – 关闭 0 – 开启，默认开启',
match_price_status                                     int             comment '分匹配状态 0 开启，要求精确系数>= 短语系数>=广泛系数，且三个比例系数均不能为空 1关',
snapshot_date                                          int             comment '快照日期',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_sogou_unit'
;
