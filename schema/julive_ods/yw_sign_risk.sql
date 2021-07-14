drop table if exists ods.yw_sign_risk;
create external table ods.yw_sign_risk(
id                                                     bigint          comment '',
business_id                                            bigint          comment '对应的业务id: 认购id/草签id/回款跟进id',
order_id                                               bigint          comment '订单表id',
deal_id                                                bigint          comment '成交单表id',
type                                                   int             comment '类型:1认购，2草签，3回款跟进',
is_audit                                               int             comment '咨询师主管是否审核:1未审核，2已审核,3驳回',
is_have_risk                                           int             comment '客户签约是否有风险:1是，2否',
imgs                                                   string          comment '上传图片',
audios                                                 string          comment '录音',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
change_log                                             string          comment '变更历史',
plan_sign_datetime                                     int             comment '预计签约时间',
status                                                 int             comment '是否删除:0未删除1已删除',
note                                                   string          comment '风险补充说明',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_sign_risk'
;
