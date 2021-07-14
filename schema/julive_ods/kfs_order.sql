drop table if exists ods.kfs_order;
create external table ods.kfs_order(
id                                                     int             comment '订单id',
developer_id                                           int             comment '开发商id',
project_id                                             bigint          comment '楼盘id',
user_realname                                          string          comment '用户姓名',
user_mobile                                            string          comment '手机号',
sex                                                    int             comment '性别 1:男 2:女',
employee_id                                            int             comment '员工id',
first_distribute_datetime                              int             comment '首次分配时间',
distribute_datetime                                    int             comment '分配时间',
intent                                                 int             comment '是否关闭 0:关 1:开',
intent_low_datetime                                    int             comment '订单关闭时间',
city_id                                                int             comment '城市id',
source                                                 int             comment '订单来源 1.小程序留电 2.h5留电 3.人工录入4.app导流订单 5客服导流订单 6 平台引导-pc-pc产生的一类订单 7. 平台引导-m-m产生的一类订单 8.客服引导-app-app产生的二类订单 9.客服引导-pc-pc产生的二类订单 10.客服引导-m-m产生的二类订单11客服引导-400-400来电经过客服引导 12 外部获客-由外投页面产生的开发商订单',
status                                                 int             comment '订单状态 10:未分配 20:已分配主管 30:已分配',
manager_distribute_datetime                            int             comment '主管分配时间',
manager_first_distribute_datetime                      int             comment '主管首次分配时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
appoint_employee                                       int             comment '指定置业顾问分配',
user_id                                                bigint          comment '用户id',
op_type                                                int             comment '',
talking_count                                          int             comment '通话次数',
talking_duration_count                                 int             comment '总的通话时长',
share_count                                            int             comment '资料包数量',
channel_id                                             int             comment '渠道表的id',
initial_create_datetime                                int             comment '最初的创建时间-从老表同步过来，标识生成订单时的创建间',
initial_update_datetime                                int             comment '最初的更新时间-从老表同步过来，标识生成订单时的更新时间',
unique_code                                            string          comment '400录音唯一标识',
is_free                                                int             comment '是否免费 1-是 2-否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_order'
;
