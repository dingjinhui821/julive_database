drop table if exists ods.dsp_jrtt_ad_group_report;
create external table ods.dsp_jrtt_ad_group_report(
id                                                     int             comment 'id',
account_id                                             int             comment '账户id',
report_date                                            int             comment '执行数据时间前一天时间',
advertiser_id                                          bigint          comment '广告主id',
campaign_id                                            bigint          comment '广告组id',
campaign_name                                          string          comment '广告组名称',
`show`                                                 int             comment '展示量',
click                                                  int             comment '点击量',
`convert`                                              int             comment '转化量',
cost                                                   double          comment '账面花费',
active                                                 int             comment '应用下载-激活量',
ctr                                                    double          comment '点击率',
cpc                                                    double          comment '平均点击单价',
cvr                                                    double          comment '点击激活转化率',
cpa                                                    double          comment '激活转化成本',
creator                                                int             comment '操作人',
updator                                                int             comment '最后一次操作人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) partitioned by(pdate string comment '数据日期')
row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_jrtt_ad_group_report'
;
