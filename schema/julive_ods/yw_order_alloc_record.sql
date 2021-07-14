drop table if exists ods.yw_order_alloc_record;
create external table ods.yw_order_alloc_record(
id                                                     bigint          comment '',
order_id                                               bigint          comment '订单id',
employee_id_now                                        bigint          comment '分配后所属咨询师id',
employee_id_old                                        bigint          comment '分配前所属咨询师id',
type                                                   int             comment '操作类型:1新建，2重新分配，3退分配，4批量修改咨询师',
distr_reason                                           int             comment '1修改咨询师选择分配原因离职或晋升2其它',
alloc_datetime                                         int             comment '分配时间',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
reincustomer_type                                      int             comment '重新上户的方式（小于0:非重新上户，1~100:无意向重新上户100~200:不分配重新上户）:-1非重新上户，1联系咨询师，2联系客服，3客服无意向回访',
reincustomer_remark                                    string          comment '重新上户说明',
channel_id                                             int             comment '对应渠道表的id',
source                                                 int             comment '1:pc主动注册 2:微信主动注册  3:pc预约看房  4:微信预约看房  5:后台业务员录入订单  6:后台业务员新增用户  7:pc免费通话 8:买房红包预留手机号 9:400来电 10:端口来电 11:微信留言 12:在线客服 13:友介 14:其他 15:关注楼盘价格留手机号',
user_id                                                bigint          comment '用户id',
device_type                                            int             comment '上户设备类型:0未知，1pc，2移动',
follow_service                                         string          comment '跟进客服',
user_cookie                                            string          comment '用户的cookie',
order_status                                           int             comment '当前订单状态',
order_alloc_type                                       int             comment '订单分配成功是所属分配类型:100:路径缩短-指定问答分配 101:神秘客户 102:路径缩短-特殊线索分配 103:路径缩短-rank分配 104:路径缩短-avg分配 105:客服创建订单指定分配 106:特殊线索分配 107:rank分配 108:avg分配',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_order_alloc_record'
;
