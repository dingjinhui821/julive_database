drop table if exists ods.hj_jrtt_change_relation;
create external table ods.hj_jrtt_change_relation(
id                                                     int             comment 'id',
account_id                                             int             comment '账户id',
account_name                                           string          comment '账户名',
advert_group_id                                        bigint          comment '广告组id',
pack_id                                                int             comment '包id :yw_android_pack 表id',
plan_prefix                                            string          comment '计划名前缀',
change_operation_id                                    int             comment '转化操作id hj_jrtt_change_operation 表id',
upload_status                                          int             comment '上传状态 0未请求 1成功 2失败',
toutiao_change_id                                      bigint          comment '今日头条的转化id',
supply_plan_name                                       string          comment '补全计划名',
batch_create_status                                    int             comment '批量新建状态 0未处理 1成功 2失败',
batch_create_plan_id                                   int             comment '批量新建广告计划id hj_jrtt_batch_create_plan 表id',
toutiao_ad_plan_id                                     bigint          comment '头条广告计划id(批量创建广告计划调用接口后补全)',
parent_id                                              int             comment '被复制主键id',
creative_status                                        int             comment '创意创建状态 1 已创建 0 未创建',
change_fail_reason                                     string          comment '转化创建失败原因',
plan_fail_reasion                                      string          comment '计划创建失败原因',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_jrtt_change_relation'
;
