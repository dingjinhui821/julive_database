drop table if exists ods.cw_supplier;
create external table ods.cw_supplier(
id                                                     int             comment '',
supplier                                               string          comment '企业名称',
supplier_type                                          int             comment '供应商类型 1 企业 2 个人',
credit_code                                            string          comment '统一社会信用代码',
type                                                   int             comment '类型(1:合资，2:独资，3:国有，4:私营5:全民所有制，6:集体所有制，7:股份制，8:有限责任)',
address                                                string          comment '住所',
legal_representative                                   string          comment '法定代表人',
register_equity                                        double          comment '注册资产（单位:万元）',
register_time                                          int             comment '成立日期',
business_scope                                         string          comment '主要经营范围',
is_taxpayers                                           int             comment '是否为一般纳税人(1-否，2-是)',
is_complete                                            int             comment '供应商材料是否完成(1-否，2-是)',
credit_record                                          string          comment '信用记录',
contact                                                string          comment '联系人',
contact_tel                                            string          comment '联系方式',
custom_filed                                           string          comment '自定义字段',
file_data                                              string          comment '资质证件和营业执照',
is_contract                                            int             comment '是否关联合同（1否 ， 2 是）',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
status                                                 int             comment '状态(1 正常0 删除)',
supplier_idcard                                        string          comment '身份证号码',
supplier_sex                                           string          comment '性别',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_supplier'
;
