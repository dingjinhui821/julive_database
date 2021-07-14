drop table if exists ods.dsp_supplement_pull_record;
create external table ods.dsp_supplement_pull_record(
id                                                     int             comment 'id',
table_name                                             string          comment '影响表名',
media_type                                             int             comment '媒体',
product_type                                           int             comment '产品形态，1:feed，2:导航,3:app渠道,4:sem',
pull_report_date                                       int             comment '补拉取日期',
pull_succ_time                                         int             comment '补拉取成功时间',
pull_record_range                                      int             comment '1-天全量拉取 2-天内部分数据拉取 3-全量拉取数据表',
is_general_task                                        int             comment '是否日常采集任务,1-日常任务 2-补数任务',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_supplement_pull_record'
;
