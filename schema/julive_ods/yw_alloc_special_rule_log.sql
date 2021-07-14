drop table if exists ods.yw_alloc_special_rule_log;
create external table ods.yw_alloc_special_rule_log(
id                                                     int             comment '',
department_id                                          int             comment '咨询师组表id',
employee_id                                            int             comment '咨询师id',
do_datetime                                            int             comment '分配日期',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
city_id                                                int             comment '城市id',
order_id                                               int             comment '订单id',
special_rule_id                                        int             comment '分配表id',
alloc_policy                                           int             comment '分配策略 1:a组 2:b组',
is_short_alloc                                         int             comment '1后台客服创建订单分配，2路径缩短分配',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_alloc_special_rule_log'
;
