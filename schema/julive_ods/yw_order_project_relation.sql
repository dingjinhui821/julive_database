drop table if exists ods.yw_order_project_relation;
create external table ods.yw_order_project_relation(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
recommend_project                                      string          comment '推荐楼盘id，用逗号隔开',
see_project                                            string          comment '带看楼盘id，用逗号隔开',
subscribe_project                                      string          comment '认购楼盘id，用逗号隔开',
sign_project                                           string          comment '签约楼盘id，用逗号隔开',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '更新时间',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_project_relation'
;
