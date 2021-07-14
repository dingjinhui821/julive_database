drop table if exists ods.cw_accounts;
create external table ods.cw_accounts(
id                                                     int             comment '',
city_id                                                int             comment '城市id(0为总部，暂定)',
account_name                                           string          comment '账户名称',
account_number                                         string          comment '账号',
account_type                                           string          comment '账户性制质',
company_id                                             int             comment '账户所属公司主体',
money                                                  double          comment '账户余额',
init_money                                             double          comment '初始资金账务余额（单位为分）',
creator                                                int             comment '',
updator                                                int             comment '',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
bank                                                   int             comment '1.平安 2.招商 3.其他',
bank_city_code                                         string          comment '银行城市代码',
bank_subsidiary_area                                   int             comment '分行地区码',
bank_power                                             string          comment '银企直连拥有哪些权限多选 银企直连功能1.查流水2.付款',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_accounts'
;
