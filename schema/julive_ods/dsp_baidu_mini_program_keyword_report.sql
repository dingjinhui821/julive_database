drop table if exists ods.dsp_baidu_mini_program_keyword_report;
create external table ods.dsp_baidu_mini_program_keyword_report(
id                                                     int             comment 'id',
account                                                string          comment '账户',
account_id                                             int             comment '账户在dsp_account表里对应的id',
plan_id                                                bigint          comment '计划id',
plan_name_status                                       string          comment '计划名称和状态',
unit_id                                                bigint          comment '单元id',
unit_name_status                                       string          comment '单元名称和状态',
word_id                                                bigint          comment '关键词id',
word_status                                            string          comment '关键词和状态',
mini_program_url                                       string          comment '小程序 url',
`show`                                                 int             comment '展示量',
click                                                  int             comment '点击量',
cost                                                   double          comment '消费',
ctr                                                    double          comment '消费',
cpc                                                    double          comment 'cpc',
pull_date                                              int             comment '拉取日期',
creator                                                bigint          comment '创建人',
updator                                                bigint          comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/dsp_baidu_mini_program_keyword_report'
;
