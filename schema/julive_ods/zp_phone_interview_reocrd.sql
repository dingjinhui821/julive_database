drop table if exists ods.zp_phone_interview_reocrd;
create external table ods.zp_phone_interview_reocrd(
id                                                     int             comment '',
order_id                                               bigint          comment '订单表id',
phone_datetime                                         int             comment '电话面试时间',
type                                                   int             comment '是否通过 1 是 2 否',
is_have_first_interview_time                           int             comment '是否预约一面 1 是 2 否',
phone_interviewer_id                                   int             comment '电话面试人员id',
first_interview_datetime                               int             comment '预约一面时间',
first_interview_number                                 int             comment '一面时间段',
evaluate                                               string          comment '电面评价',
base_score                                             int             comment '基础得分',
additional_score                                       int             comment '加分',
total_score                                            double          comment '总分',
p_id                                                   int             comment '模板id',
config_json_data                                       string          comment '提交配置数据',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_phone_interview_reocrd'
;
