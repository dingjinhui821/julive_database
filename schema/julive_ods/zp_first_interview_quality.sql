drop table if exists ods.zp_first_interview_quality;
create external table ods.zp_first_interview_quality(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
communicative_ablity                                   double          comment '沟通表达能力评分',
affinity_ablity                                        double          comment '亲和能力评分',
hardship_ablity                                        double          comment '吃苦抗压评分',
execute_ablity                                         double          comment '目标执行力评分',
is_agree_corporate_culture                             int             comment '是否认同公司文化与价值观 0未知 1 是 2 否',
is_accept_sales_work                                   int             comment '是否接受销售工作 0未知 1 是 2 否',
is_accept_high_intensity_work                          int             comment '是否接受高强度工作 0未知 1 是 2 否',
is_accept_no_work_day                                  int             comment '是否接受调休制 0未知 1 是 2 否',
is_punctual                                            int             comment '是否面试准时 0未知 1 是 2 否',
is_dress_well                                          int             comment '是否面试穿着得体 0未知 1 是 2 否',
is_politeness                                          int             comment '是否待人有礼貌 0未知 1 是 2 否',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_first_interview_quality'
;
