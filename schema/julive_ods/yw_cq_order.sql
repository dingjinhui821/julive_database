drop table if exists ods.yw_cq_order;
create external table ods.yw_cq_order(
id                                                     bigint          comment '主键id',
city_id                                                int             comment '城市id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
order_id                                               bigint          comment '订单id',
decision                                               int             comment '决策:1:通过，0:拒绝',
new_score                                              int             comment '最终留电分值',
score                                                  bigint          comment '得分',
order_desc                                             string          comment '描述',
event_time                                             bigint          comment '事件时间戳(毫秒)',
process_time                                           bigint          comment '完成时间戳(毫秒)',
customer_intent_city                                   bigint          comment '意向城市',
product_id                                             int             comment '项目id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_cq_order'
;
