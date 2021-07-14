drop table if exists ods.cj_user_pay;
create external table ods.cj_user_pay(
id                                                     int             comment '主键',
booking_id                                             int             comment '预订单id',
mch_id                                                 string          comment '商户号',
add_type                                               int             comment '添加类型: 0正常支付 1手动添加',
payer                                                  int             comment '支付人账号',
pay_type                                               int             comment '支付类型 1微信 2支付宝',
pay_money                                              int             comment '支付意向金金额(单位:分)',
trade_order_num                                        string          comment '流水号',
pay_time                                               int             comment '支付时间',
pay_account                                            string          comment '支付人三方账号',
plat_trade_no                                          string          comment '三方订单号',
pay_status                                             int             comment '状态:1待支付 5已支付 10退款成功',
refund_order_num                                       string          comment '退单号id',
refund_time                                            int             comment '退款时间',
create_datetime                                        int             comment '创建时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_user_pay'
;
