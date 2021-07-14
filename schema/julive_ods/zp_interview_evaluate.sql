drop table if exists ods.zp_interview_evaluate;
create external table ods.zp_interview_evaluate(
id                                                     int             comment '',
order_id                                               bigint          comment '订单id',
answer_one                                             int             comment '面试官在初试中是否清晰地介绍了居理新房的基本概况及运营模式 (1、是、2、否、3、只说清楚一部分）',
answer_two                                             int             comment '关于咨询师岗位的工作性质，面试官在初试中是否提到这三点:a.带有一定的销售性质，多劳多得、工作强度较大b.工作强度较大c.员工成长速度较快(1、是、2、否、3、只说清楚一部分）',
answer_three                                           int             comment '关于咨询师岗位的作息安排，面试官在初试中是否提到这三点:a.购房客户多集中于周末、节假日看房b.周末和除春节以外的法定节假日需正常工作c.该岗位安排周中调休(1、是、2、否、3、只说清楚一部分）',
answer_four                                            int             comment '面试官在面试中是否清楚回答您对公司及岗位的疑问 (1、是、2、否、3、只说清楚一部分）',
answer_five                                            int             comment '请问您对参加居理面试全流程的整体感受是(10、11、12、13、14、15)',
answer_six                                             string          comment '请问您对居理面试官或居理招聘全流程是否还有其他的建议或意见？',
answer_json                                            string          comment '模板数据',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
p_id                                                   int             comment '模板id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/zp_interview_evaluate'
;
