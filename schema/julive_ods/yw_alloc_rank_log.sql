drop table if exists ods.yw_alloc_rank_log;
create external table ods.yw_alloc_rank_log(
id                                                     int             comment '',
employee_id                                            int             comment '咨询师id',
do_datetime                                            int             comment '分配日期',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
alloc_rank_count_day                                   int             comment '当前分配次数',
city_id                                                int             comment '城市id',
type                                                   int             comment '类型:1正常分配 2补单',
rank                                                   int             comment '排名',
order_id                                               int             comment '',
is_short_alloc                                         int             comment '1,后台客服创建订单记录，2路径缩短记录',
alloc_policy                                           int             comment '分配策略 1:a组 2:b组',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_alloc_rank_log'
;
