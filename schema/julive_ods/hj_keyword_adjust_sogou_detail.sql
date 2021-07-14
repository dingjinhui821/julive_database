drop table if exists ods.hj_keyword_adjust_sogou_detail;
create external table ods.hj_keyword_adjust_sogou_detail(
id                                                     int             comment 'id',
adjust_id                                              int             comment '关键词调整主表id',
media_type                                             int             comment '媒体类型（百度:1360:2搜狗:3今日头条:4）',
account                                                string          comment '账户',
account_id                                             int             comment '账户在dsp_account表里对应的id',
plan_id                                                bigint          comment '计划id',
plan_name                                              string          comment '计划名称',
unit_id                                                bigint          comment '单元id',
unit_name                                              string          comment '单元名称',
keyword_id                                             bigint          comment '关键词id',
keyword                                                string          comment '关键词',
adjust_type                                            int             comment '调整类型 1出价/2匹配模式/3开启/4暂停/5删除/6添否',
old_info                                               string          comment '原信息',
new_info                                               string          comment '更新信息',
opt_status                                             int             comment '操作状态(0未上传,1上传中[异步中间状态],2上传成功,3上传失败)',
fail_msg                                               string          comment '失败原因',
retry_num                                              int             comment '重试次数',
is_delete                                              int             comment '是否删除  0整除  1已删除',
unique_id                                              string          comment '提交唯一标示id',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_keyword_adjust_sogou_detail'
;
