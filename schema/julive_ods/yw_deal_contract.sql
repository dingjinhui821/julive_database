drop table if exists ods.yw_deal_contract;
create external table ods.yw_deal_contract(
id                                                     int             comment 'id',
deal_id                                                int             comment '成交单id',
status                                                 int             comment '状态 1是 2否',
type                                                   int             comment '类型5首付是否交齐,10贷款资料是否提交,15贷款资料审核是否通过,20贷款审批是否通过,25按揭合同是否签署,30全款到帐,35甲方是否确认业绩,40平台是否收到佣金,45全款分期（无比例）,50全款分期（有比例),250网签首付交齐贷款资料交齐且网签备案完成',
plan_time                                              int             comment '预计时间',
real_time                                              int             comment '实际时间',
plan_percent                                           double          comment '预计付款比例',
real_percent                                           double          comment '实际付款比例',
times                                                  int             comment '分期次数 1 第1次,2 第2次,3 第3次,4 第4次',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '修改人',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_deal_contract'
;
