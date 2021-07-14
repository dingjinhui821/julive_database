drop table if exists julive_dim.dict_budget_range;
create external table julive_dim.dict_budget_range(
id                                                     int             comment '',
city_id                                                int             comment '城市id',
city_name                                              string          comment '城市名称',
grade_id                                               int             comment '预算等级id',
grade_name                                             string          comment '预算等级',
low_budget                                             bigint          comment '预算下限',
high_budget                                            bigint          comment '预算上限',
create_time                                            string          comment '创建时间',
update_time                                            string          comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_dim/dict_budget_range'
;
