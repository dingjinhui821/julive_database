drop table if exists ods.market_activity_rules_stat;
create external table ods.market_activity_rules_stat(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
market_activity_id                                     int             comment '活动id',
market_activity_title                                  string          comment '活动主题',
rule_id                                                int             comment '规则id',
order_id                                               bigint          comment '订单id',
user_id                                                bigint          comment '用户id',
order_create_time                                      int             comment '订单创建时间',
distribute_datetime                                    int             comment '订单分配时间',
price_max                                              double          comment '最高总价',
price_min                                              double          comment '最低总价',
district_name                                          string          comment '区域',
project_type                                           string          comment '业态',
start_datetime                                         int             comment '规则开始时间',
end_datetime                                           int             comment '规则结束时间',
status                                                 int             comment '订单状态',
intent                                                 int             comment '订单是否关闭，1关闭，3未关闭',
employee_realname                                      string          comment '订单所属咨询师',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/market_activity_rules_stat'
;
