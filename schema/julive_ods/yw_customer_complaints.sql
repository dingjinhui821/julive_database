drop table if exists ods.yw_customer_complaints;
create external table ods.yw_customer_complaints(
id                                                     int             comment '客诉问题主键id',
order_id                                               int             comment '订单表id',
employee_id                                            int             comment '咨询师 ||客服 id',
employee_leader_id                                     int             comment '咨询师||客服 主管id',
complaints_type                                        int             comment '问题类型（1:订单问题 2:客服问题 3:其他问题）',
city_id                                                int             comment '城市id',
problem_cat_level_one                                  int             comment '一级问题分类（1:客诉问题 2:红黄线问题）',
problem_cat_level_two                                  int             comment '二级问题分类（一级:「1:投诉 2:售后 6:售后_客户异议 7:售后_更换咨询师 8:售后_认购后问题 」 2:「3:红线 4:黄线 5:橙线 」）',
feedback_channel                                       int             comment '反馈渠道（1:内部员工反馈 2:邮箱收件 3:客户反馈 4:品控组质检 5:咨询部反馈）',
complaint_status                                       int             comment '客诉状态（0:正常 1:作废）',
influencing_achievements_des                           string          comment '绩效影响',
processing_result_des                                  string          comment '处理结果',
creator                                                int             comment '创建者id',
updator                                                int             comment '更新者id',
processing_result_datetime                             int             comment '处理结果日期',
complaints_occur_datetime                              int             comment '客诉发生日期',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_customer_complaints'
;
