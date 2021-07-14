drop table if exists ods.xpt_order;
create external table ods.xpt_order(
id                                                     int             comment '订单id',
user_id                                                bigint          comment '用户id',
user_realname                                          string          comment '用户姓名',
user_mobile                                            string          comment '手机号',
require_type                                           int             comment '需求类型 1 不限 2 新房 3 二手房',
is_distribute                                          int             comment '是否已分配  1 是  2 否',
store_id                                               int             comment '门店id',
store_user_id                                          int             comment '经纪人id',
distribute_datetime                                    int             comment '分配时间',
distribute_fail_type                                   int             comment '分配失败类型',
city_id                                                int             comment '城市id',
status                                                 int             comment '订单状态 0 无效订单  1 有效状 态',
source                                                 int             comment '订单来源 1.c端获客  2.平台给客',
source_class                                           int             comment '订单来源子类型  101 帮我找房 102 客服线索分配 103 咨询师关闭订单 104 关闭订单客服跟进 105 客服手动录入订单',
sex                                                    int             comment '性别 1 男  2女',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/xpt_order'
;
