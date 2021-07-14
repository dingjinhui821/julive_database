drop table if exists ods.yw_sign_business_tags;
create external table ods.yw_sign_business_tags(
id                                                     bigint          comment '',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
order_id                                               bigint          comment '订单id',
employee_id                                            int             comment '咨询师id',
sign_id                                                bigint          comment '网签标签表',
label_type                                             int             comment '标签类型(1:预测标签2:延迟标签3:直接录入标签)',
label_value                                            int             comment '标签名（101:网签预判成功102:网签预判失败103:网签预判成功率过低）',
deal_id                                                bigint          comment '成交单id',
label_name                                             string          comment '标签名称',
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
location '/dw/ods/yw_sign_business_tags'
;
