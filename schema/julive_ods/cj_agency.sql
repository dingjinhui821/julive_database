drop table if exists ods.cj_agency;
create external table ods.cj_agency(
id                                                     bigint          comment '',
agency_name                                            string          comment '渠道名称(例:iqiyi_5)',
agency_desc                                            string          comment '渠道描述(例:爱奇艺5)',
app_id                                                 int             comment 'app_id',
group_id                                               int             comment '组id',
status                                                 int             comment '状态，1启用，2禁用',
create_datetime                                        int             comment '',
update_datetime                                        int             comment '',
creator                                                bigint          comment '',
updator                                                bigint          comment '',
channel_type_id                                        int             comment '媒体分类id',
channel_type_name                                      string          comment '媒体分类名称',
utm_source                                             string          comment '用来标记广告来源',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_agency'
;
