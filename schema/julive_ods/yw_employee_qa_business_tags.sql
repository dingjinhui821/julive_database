drop table if exists ods.yw_employee_qa_business_tags;
create external table ods.yw_employee_qa_business_tags(
id                                                     bigint          comment '',
employee_id                                            bigint          comment '咨询师id',
employee_manage_id                                     bigint          comment '咨询师主管id',
question_id                                            int             comment '问题id',
order_id                                               int             comment '订单id',
label_type                                             int             comment '标签类型:4001:咨询师问答标签',
label_value                                            int             comment '标签值:500:问答质量不达标501:问答数量不达标 ',
label_name                                             string          comment '标签名称',
tag_time                                               int             comment '打标签时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
is_cancel                                              int             comment '0 未申诉取消 1 已申诉取消',
business_datetime                                      int             comment '打标签业务时间',
deduct_score                                           int             comment '扣除分数',
cancel_employee                                        int             comment '取消人',
cancel_datetime                                        int             comment '取消时间',
employee_leader_id                                     int             comment '咨询师主管',
city_id                                                int             comment '城市id',
cancel_type                                            int             comment '取消类型（1-系统取消、2-申诉取消、3-人工取消、4-脚本取消）',
cancel_console                                         string          comment '脚本取消、执行的脚本及方法名称',
total_num                                              int             comment '总数量',
finish_num                                             int             comment '完成数量',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_qa_business_tags'
;
