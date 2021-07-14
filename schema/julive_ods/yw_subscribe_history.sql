drop table if exists ods.yw_subscribe_history;
create external table ods.yw_subscribe_history(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
subscribe_id                                           bigint          comment '认购表id',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '员工id',
user_id                                                bigint          comment '用户id',
subscribe_name                                         string          comment '认购人姓名',
subscribe_phone                                        string          comment '认购人电话',
subscribe_identity_number                              string          comment '认购人身份证号',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
house_type                                             int             comment '户型',
acreage                                                int             comment '面积',
house_number                                           string          comment '房号',
subscribe_datetime                                     int             comment '认购时间',
deal_money                                             int             comment '成交金额',
plan_sign_datetime                                     int             comment '预计签约时间',
note                                                   string          comment '备注',
status                                                 int             comment '认购状态: 1:已认购  2:退认',
serialize_data                                         string          comment '认购序列化的数据',
chargeback_reason                                      string          comment '退认购原因',
sign_possibility_reason                                string          comment '签约可能性原因说明',
first_percent                                          double          comment '首付比例',
source_site                                            string          comment '来源站点',
source_ip                                              string          comment '来源服务器ip',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_subscribe_history'
;
