drop table if exists ods.yw_order_wechat;
create external table ods.yw_order_wechat(
id                                                     bigint          comment '主键',
order_id                                               bigint          comment '订单id',
employee_id                                            int             comment '咨询师id',
city_id                                                int             comment '城市id',
wx_num                                                 string          comment '微信号',
wx_id                                                  string          comment '微信id',
status                                                 int             comment '审核状态，和质检表同步',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
source                                                 int             comment '详见配置常量',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_wechat'
;
