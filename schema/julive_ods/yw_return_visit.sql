drop table if exists ods.yw_return_visit;
create external table ods.yw_return_visit(
id                                                     bigint          comment 'id',
order_id                                               bigint          comment '',
visit_employee_id                                      bigint          comment '回访人员id',
visit_employee_name                                    string          comment '',
visit_datetime                                         int             comment '',
note                                                   string          comment '',
status                                                 int             comment '状态:1已回访,2回访失败,3放弃回访,4未回访',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
visit_num                                              int             comment '回访次数',
employee_grade                                         int             comment '0非常不满意 1不满意 3一般 4满意 5非常满意',
continue_server                                        int             comment '是否仍需要侃家服务:0未知，1不需要，2继续服务',
employee_id                                            bigint          comment '咨询师id',
employee_adjust_city                                   bigint          comment '咨询师核算城市',
employee_name                                          string          comment '咨询师姓名',
result_types                                           bigint          comment '1未接听,2无效号码,3直接挂掉,4只关注二手房,5已买房,6不考虑买房了,7拒绝侃家服务,8仍需要服务（咨询师未指定）,9仍需要服务（咨询师已指定）,10拒绝回访,11其他',
keep_follow                                            int             comment '0: 默认，咨询师不继续跟进 1: 咨询师继续跟进',
employee_leader_id                                     bigint          comment '咨询师主管id',
employee_leader_adjust_city                            bigint          comment '咨询师主管核算城市',
employee_leader_realname                               string          comment '咨询师主管姓名',
buy_house_project_name                                 string          comment '已购房的楼盘名称',
comment_labels                                         string          comment '评价标签',
sm_update_grade                                        int             comment '客服主管是否修改已回访的关闭订单的满意度，1，否，2，是',
sm_id                                                  bigint          comment '修改已回访的关闭订单满意度的客服主管',
sm_update_grade_datetime                               int             comment '客服主管修改已回访的关闭订单时间',
type                                                   int             comment '评论类型:1客户2客服',
city_id                                                int             comment '城市id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_return_visit'
;
