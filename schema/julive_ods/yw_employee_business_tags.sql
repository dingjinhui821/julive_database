drop table if exists ods.yw_employee_business_tags;
create external table ods.yw_employee_business_tags(
id                                                     bigint          comment 'id',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
label_value                                            int             comment '标签名（1:预判认购成功率过低2:预判网签成功率过低）',
label_name                                             string          comment '标签名称',
business_value                                         string          comment '标签业务值',
employee_id                                            int             comment '咨询师id',
employee_name                                          string          comment '咨询师名称',
step                                                   int             comment '30:认购,70:签约',
order_id                                               bigint          comment '订单id',
label_type                                             int             comment '标签分类:1预测标签2延迟标签3:直接录入',
sign_id                                                bigint          comment '网签id',
is_cancel                                              int             comment '0 未申诉取消 1 已申诉取消',
business_datetime                                      int             comment '打标签业务时间',
deduct_score                                           int             comment '扣除分数',
cancel_employee                                        int             comment '取消人',
cancel_datetime                                        int             comment '取消时间',
employee_leader_id                                     int             comment '咨询师主管',
city_id                                                int             comment '城市id',
cancel_type                                            int             comment '取消类型（1-系统取消、2-申诉取消、3-人工取消、4-脚本取消）',
cancel_console                                         string          comment '脚本取消、执行的脚本及方法名称',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_employee_business_tags'
;
