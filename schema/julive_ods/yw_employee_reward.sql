drop table if exists ods.yw_employee_reward;
create external table ods.yw_employee_reward(
id                                                     int             comment '',
money                                                  int             comment '打赏金额,单位是分',
order_id                                               bigint          comment '支持系统对应的order_id',
trade_no                                               string          comment '商户订单号,唯一的,和微信交互的基础(订单方向)',
create_datetime                                        int             comment '打赏创建时间',
update_datetime                                        int             comment '打赏更新时间,',
status                                                 int             comment '1:未支付,2:支付中,3:已支付,4:支付失败',
asyn_datetime                                          int             comment '微信异步通知时间',
employee_id                                            bigint          comment '咨询师id',
nick_name                                              string          comment '用户昵称',
from_url                                               string          comment '来源url',
send_time                                              int             comment '消息发送时间',
business_id                                            string          comment '针对business_type下的id做的打赏',
business_type                                          string          comment '打赏的业务模块',
user_id                                                bigint          comment '居里系统的user_id',
transaction_id                                         string          comment '微信支付订单号',
time_end                                               string          comment '支付完成时间',
wx_pay_result                                          string          comment '支付后微信返回的数据,xml 字符串',
type                                                   string          comment 'wx:公众号,wx-h5:微信h5,miniapp:小程序支付',
decrypt_business_id                                    int             comment '解密后的带看id(business_id)',
open_id                                                string          comment '微信open_id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_reward'
;
