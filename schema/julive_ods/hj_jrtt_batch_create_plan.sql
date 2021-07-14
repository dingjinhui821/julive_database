drop table if exists ods.hj_jrtt_batch_create_plan;
create external table ods.hj_jrtt_batch_create_plan(
id                                                     int             comment '主键id',
change_operation_id                                    int             comment '转化操作id',
app_type                                               string          comment '设备类型',
download_type                                          string          comment '下载方式',
delivery_range                                         string          comment '投放范围',
pricing                                                string          comment '投放目标',
city                                                   string          comment '地域',
gender                                                 string          comment '性别',
age                                                    string          comment '年龄区间',
retargeting_tags_include                               string          comment '自定人群',
ad_tag                                                 string          comment '兴趣分类',
interest_tags                                          string          comment '兴趣关键词',
ac                                                     string          comment '网络',
app_behavior_target                                    string          comment 'app行为',
budget_mode                                            string          comment '预算类型',
budget                                                 double          comment '预算',
schedule_type                                          string          comment '投放时间类型',
schedule_time                                          string          comment '广告投放时段',
flow_control_mode                                      string          comment '投放方式',
cpa_bid                                                double          comment '目标转化出价',
status                                                 int             comment '状态:0未处理,1 成功,2失败',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_jrtt_batch_create_plan'
;
