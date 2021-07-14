drop table if exists ods.yw_follow_business_tag;
create external table ods.yw_follow_business_tag(
id                                                     bigint          comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
order_id                                               bigint          comment '订单id',
business_id                                            bigint          comment '跟进id',
follow_type                                            int             comment '跟进类型(1=联系，2=带看)',
content                                                string          comment '其他信息根据不同类型记录: 未签到楼盘 没有位置的楼盘',
employee_id                                            int             comment '标签对象 员工id',
label_type                                             int             comment '标签类型(常量写在ywfollowbusinesstag)',
label_val                                              int             comment '标签值',
label_name                                             string          comment '标签名称',
is_old                                                 int             comment '是否老数据 0:否 1:是 2无打点',
status                                                 int             comment '是否删除:1未删除-1:已删除',
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
location '/dw/ods/yw_follow_business_tag'
;
