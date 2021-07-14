drop table if exists ods.cw_contract_approval;
create external table ods.cw_contract_approval(
id                                                     int             comment '',
approval_id                                            int             comment '审批id',
contract_name                                          string          comment '合同名称',
partner_name                                           string          comment '合作方名称多个用逗号分隔',
partner_id                                             string          comment '合作方id多个逗号分隔',
partner_subject                                        string          comment '合作标的',
is_frame_contract                                      int             comment '是否是框架合同 1框架类合同 2非框架类合同',
contract_type                                          int             comment '合同类型 1采购 2推广广告 3技术服务 4业务 5其他',
city_id                                                int             comment '使用印章所属城市id',
seal_type                                              string          comment '用印类型 1财务专用章 2法人章 3合同章 4公章',
contract_price                                         double          comment '合同金额',
cooperation_start_time                                 int             comment '合作开始时间',
cooperation_end_time                                   int             comment '合作结束时间',
note                                                   string          comment '条款备注',
use_seal_time                                          int             comment '用印时间',
seal_name                                              string          comment '印章名称',
seal_executor                                          string          comment '用印执行人',
seal_id                                                string          comment '印章id',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
company_id                                             int             comment '公司主体id',
company_name                                           string          comment '公司主体名字',
business_id                                            int             comment '业务id',
business_type                                          int             comment '业务类型 1签约单',
seal_is_takeout                                        int             comment '印章是否外带1.外带2.不外带',
is_special_approval                                    int             comment '是否特批1.是2.否',
is_our_contract_tpl                                    int             comment '是否使用我方合同1.使用2不使用',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cw_contract_approval'
;
