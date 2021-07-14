drop table if exists ods.hj_jrtt_creative;
create external table ods.hj_jrtt_creative(
id                                                     int             comment '主键自增',
account_id                                             int             comment '账户id',
ad_position_type                                       int             comment '广告位置类型',
ad_media                                               string          comment '广告指定媒体',
creative_type                                          string          comment '创意方式1:自定义2:程序化创意',
creative_info                                          string          comment '创意信息json',
app_name                                               string          comment '应用名',
img_to_video                                           int             comment '图片生成视频1:启用2.不启用',
ad_comment_opened                                      int             comment '广告评论1开启2关闭',
creative_cate                                          int             comment '广告分类',
creative_tag                                           string          comment '创意标签',
creative_status                                        int             comment '状态 0处理中1:成功2:失败',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
plan_template_id                                       bigint          comment '创建模板的头条计划id',
plan_status                                            string          comment '筛选的计划状态 none:不限，ad_status_delivery_ok:投放中',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_jrtt_creative'
;
