drop table if exists ods.cw_accounting_fee_detail;
create external table ods.cw_accounting_fee_detail(
id                                                     int             comment '',
link_id                                                int             comment '业务id',
city_id                                                int             comment '城市id',
depart_id                                              int             comment '部门id',
class_id                                               int             comment '会科费用类别id',
class_name                                             string          comment '费用类别名称',
first_level_sn                                         string          comment '一级科目编码',
second_level_item_id                                   int             comment '会计二级科目id',
second_level_sn                                        string          comment '会计二级科目编码',
settlement_method                                      int             comment '结算方式 1、借款、2、付款、3、冲账 4 收款(借款，付款是支出，收款是收入）',
voucher_sn                                             string          comment '凭证号',
payee                                                  string          comment '收款人',
is_pay                                                 int             comment '是否结算 1 是 2 否',
original_money                                         double          comment '原始金额',
pay_money                                              double          comment '付款金额',
pay_datetime                                           int             comment '收付款时间',
account_id                                             int             comment '付款账户',
abstract                                               string          comment '摘要',
is_delete                                              int             comment '是否删除 1 是 2 否',
type                                                   int             comment '类型 1.付款、2、交通费、3、差旅费、4、返费、5、成交奖 6 自定义 7.银行流水',
deal_check_info                                        string          comment '成交单选中信息',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
is_from_bank                                           int             comment '是否来自银企直连1是2否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_accounting_fee_detail'
;
