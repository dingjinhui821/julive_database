drop table if exists ods.yw_see_card_queue;
create external table ods.yw_see_card_queue(
id                                                     int             comment '',
project_id                                             bigint          comment '楼盘id',
order_id                                               bigint          comment '订单id',
see_id                                                 bigint          comment '带看id',
deal_type                                              int             comment '成交类型',
customer_name                                          string          comment '客户姓名',
customer_phone                                         string          comment '客户电话',
queued_time                                            int             comment '排卡时间',
guess_open_time                                        string          comment '预计开盘时间',
pick_house_rule                                        string          comment '选房规则',
remark                                                 string          comment '备注',
status                                                 int             comment '状态1正常2删除',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人id',
updator                                                int             comment '更新人id',
deal_id                                                int             comment '成交单id',
employee_id                                            bigint          comment '咨询师id',
employee_manager_id                                    bigint          comment '咨询师主管id',
city_id                                                int             comment '城市id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_see_card_queue'
;
