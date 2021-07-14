drop table if exists ods.yw_see_project_global_comment;
create external table ods.yw_see_project_global_comment(
id                                                     bigint          comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人（为空是客户评价，否则是客服回访）',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
order_id                                               bigint          comment '订单id',
see_project_id                                         bigint          comment '带看表id',
global_comment                                         string          comment '整体评论',
employee_grade                                         int             comment '0非常不满意 1不满意 3一般 4满意 5非常满意',
fail_times                                             int             comment '联系失败记录数',
employee_leader_id                                     bigint          comment '咨询师主管id',
employee_leader_realname                               string          comment '咨询师主管姓名',
visit_employee_id                                      bigint          comment '回访人员id',
visit_employee_name                                    string          comment '回访人员姓名',
visit_datetime                                         int             comment '回访时间',
see_employee_id                                        bigint          comment '带看咨询师id',
see_employee_name                                      string          comment '带看咨询师姓名',
status                                                 int             comment '状态:1已回访,2回访失败,3放弃回访,4未回访',
probability                                            int             comment '推荐侃家的概率',
comment_labels                                         string          comment '评价标签',
sm_update_grade                                        int             comment '客服主管是否修改已回访订单的满意度，1，否，2，是',
sm_id                                                  bigint          comment '修改已回访的订单满意度的客服主管',
sm_update_grade_datetime                               int             comment '客服主管修改已回访订单时间',
type                                                   int             comment '评论类型:1客户2客服',
contact_type                                           int             comment '回访联系类型（0.电话回访 1.短信回访）',
is_mystery                                             int             comment '是否是神秘客户 1.是,2.否',
b_tag_late                                             int             comment '后台带看迟到标签标记 1未标记 2标记',
c_tag_late                                             int             comment '客户评价带看迟到 1未迟到 2迟到',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_see_project_global_comment'
;
