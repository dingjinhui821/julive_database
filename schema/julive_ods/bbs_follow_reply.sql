drop table if exists ods.bbs_follow_reply;
create external table ods.bbs_follow_reply(
id                                                     int             comment '',
content                                                string          comment '回复内容',
follow_log                                             string          comment '跟进内容',
status                                                 int             comment '状态:100:未处理,110确认中120已确认问题130正在解决140无效bug/问题150已解决200评估中 205 需求池中 210需求中220开发中230已上线240作废 250转为bug  260反馈,',
expect_datetime                                        int             comment '预计跟进结束时间',
forum_id                                               bigint          comment '主表id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
next_follower_id                                       int             comment '下一步跟进人id',
change_type                                            int             comment '改变情况 1 只是状态变更 2 只是变更跟进人 3 跟进人和状态都变更',
follow_end_time                                        int             comment '实际跟进结束时间',
start_follow_datetime                                  int             comment '开始跟进时间',
is_solution                                            int             comment '是否是解决方案 1 是 2 否',
problem_depart_id                                      int             comment '责任部门id',
plan_solve_date                                        int             comment '预计解决日期',
bbs_type                                               int             comment '当天帖子类型（后补充，老数据无此字段）',
bbs_content_type                                       int             comment '当前帖子内容类型（后补充，老数据无此字段）',
is_async_end                                           int             comment '异步任务是否已完成 1 是 2 否',
solve_type                                             int             comment '确认解决类型 0 未知 1真正解决 2达成一致后解决',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_follow_reply'
;
