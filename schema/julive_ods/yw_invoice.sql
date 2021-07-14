drop table if exists ods.yw_invoice;
create external table ods.yw_invoice(
id                                                     int             comment '',
city_id                                                int             comment '城市id',
type                                                   int             comment '开票类型 1专票 2普票 3代开专票',
need_verify                                            int             comment '发票是否需要验真伪 1是 2否',
company_id                                             int             comment '开票主体公司id (cw_company_config表的id)',
transferee                                             int             comment '发票交接人(已废弃)',
expected_open_time                                     int             comment '期望开票时间',
begin_title                                            string          comment '发票抬头',
tax_number                                             string          comment '税号',
open_bank                                              string          comment '开户行',
accounts                                               string          comment '开户行帐号',
register_address                                       string          comment '注册地址',
telephone                                              string          comment '电话',
img_list                                               string          comment '一般纳税人文件照片，json',
content_type                                           int             comment '开票内容类型 1经纪代理服务*中介服务费 2经纪代理服务*咨询服务费 3经纪代理服务*服务费 4经纪代理服务*代理服务费 5鉴证咨询服务*服务费 6鉴证咨询服务*中介服务费 7其他',
content_note                                           string          comment '开票内容备注',
note                                                   string          comment '备注',
status                                                 int             comment '状态 1申请待审核 2发票已开 3申请被驳回 4作废待审核 5发票已作废 6开票撤销',
invoice_number                                         string          comment '发票号',
invoice_open_time                                      int             comment '开票时间',
tracking_number                                        string          comment '快递单号',
invoice_applyer                                        int             comment '开票申请人',
invoice_apply_time                                     int             comment '开票申请时间',
invalid_applyer                                        int             comment '作废申请人',
invalid_time                                           int             comment '作废时间',
invalid_apply_time                                     int             comment '作废申请时间',
invalid_reason                                         string          comment '作废原因',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
unit_type                                              int             comment '开票单位类型 0无 1项 2次 3其他',
unit_name                                              string          comment '开票单位名称',
attachments                                            string          comment '附件材料',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_invoice'
;
