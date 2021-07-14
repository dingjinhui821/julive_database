drop table if exists ods.yw_sign_history;
create external table ods.yw_sign_history(
id                                                     bigint          comment '',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
sign_id                                                bigint          comment '签约表id',
order_id                                               bigint          comment '订单id',
employee_id                                            bigint          comment '员工id',
user_id                                                bigint          comment '用户id',
sign_name                                              string          comment '签约人姓名',
sign_phone                                             string          comment '签约人电话',
sign_identity_number                                   string          comment '签约人身份证号',
project_id                                             bigint          comment '楼盘id',
project_name                                           string          comment '楼盘名称',
house_type                                             int             comment '户型',
acreage                                                int             comment '面积',
house_number                                           string          comment '房号',
sign_datetime                                          int             comment '签约时间',
deal_money                                             int             comment '成交金额',
plan_commission_datetime                               int             comment '预计结佣时间',
note                                                   string          comment '备注',
status                                                 int             comment '签约状态: 1:已签约 2:退签约',
serialize_data                                         string          comment '签约序列化的数据',
chargeback_reason                                      string          comment '退网签/草签原因',
sign_possibility                                       int             comment '签约可能性 1高 2中 3低',
sign_possibility_reason                                string          comment '签约可能性原因说明',
sign_type                                              int             comment '签约类型，1草签 2网签',
source_site                                            string          comment '来源站点',
source_ip                                              string          comment '来源服务器ip',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sign_history'
;
