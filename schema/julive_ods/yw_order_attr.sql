drop table if exists ods.yw_order_attr;
create external table ods.yw_order_attr(
order_id                                               bigint          comment '主键,订单id',
product_id                                             int             comment '项目id',
sub_product_id                                         int             comment '项目子id',
is_preferred_customers                                 int             comment '是否显示优选客户标识（1.是 2.否）',
create_datetime                                        int             comment '创建时间',
creator                                                int             comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                int             comment '更新人',
is_youpai                                              int             comment '是否是优派:0否，1是',
rule_type                                              int             comment '分配类型:1-路径缩短 2.优选客户 3.在线im聊天',
customer_intent_city                                   bigint          comment '客户意向城市',
order_business_category                                int             comment '订单业务分类-在c端订单创建后依据业务将订单进行分配 0: 未知1:一类2:2类3:3类',
order_in_server_system_distribute                      int             comment '系统自动将在客服阶段的订单进行上户:0-否，目前正常流程1:是，系统把客服阶段的订单进行分配上户',
comjia_platform_id                                     int             comment '平台id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_attr'
;
