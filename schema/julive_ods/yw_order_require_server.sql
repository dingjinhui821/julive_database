drop table if exists ods.yw_order_require_server;
create external table ods.yw_order_require_server(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
order_id                                               bigint          comment '订单id',
user_id                                                bigint          comment '用户id',
unit_price_min                                         int             comment '最小单价',
unit_price_max                                         int             comment '最大单价',
first_price_min                                        int             comment '最小首付',
first_price_max                                        int             comment '最大首付',
total_price_min                                        int             comment '最小总价',
total_price_max                                        int             comment '最大总价',
note                                                   string          comment '备注',
interest_project_name                                  string          comment '客户感兴趣楼盘的名称，用逗号隔开',
has_main_push_projects                                 int             comment '客户感兴趣楼盘是否有当期主推楼盘1:否2:是',
acreage_min                                            int             comment '最小面积',
acreage_max                                            int             comment '最大面积',
district_id                                            string          comment '区域id，逗号分隔',
district_other                                         string          comment '区域其他',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_require_server'
;
