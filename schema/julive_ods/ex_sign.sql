drop table if exists ods.ex_sign;
create external table ods.ex_sign(
id                                                     int             comment '签约单id',
ex_order_id                                            int             comment 'bd单id',
contract_name                                          string          comment '合同名称',
contract_begin_datetime                                int             comment '合同开始时间',
contract_end_datetime                                  int             comment '合同结束时间',
contract_type                                          int             comment '合同类型1框架合同（不含费率）、2单项目补充协议、3单项目合同',
company_id                                             int             comment '我主合同主体公司id',
partner_contract_name                                  string          comment '对方合同主体名称',
cw_supplier_id                                         int             comment 'cw_supplier表id 对方合同主体',
taxpayer_type                                          int             comment '对方纳税人资质 1一般纳税人 2小规模纳税人',
ecom_contract_type                                     int             comment '是否属于电商类收不回合同 1是 2否',
status                                                 int             comment '1条款中 2条款审批中 3条款审批通过 4合同盖章审批中 5草签 6正签',
terms_approval                                         int             comment '是否发起条款审批 1是 2否',
contract_approval_id                                   int             comment '合同盖章审批id',
terms_approval_id                                      int             comment '条款审批id',
recycling_contract                                     int             comment '合同是否回收 1已回收 2未回收',
plan_arrival_datetime                                  int             comment '预计合同到达时间',
contract_number                                        string          comment '合同编号',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
rule_approval_pass_date                                int             comment '条款审批通过时间年月日',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_sign'
;
