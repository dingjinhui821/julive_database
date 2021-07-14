drop table if exists ods.bbs_forum;
create external table ods.bbs_forum(
id                                                     bigint          comment '',
title                                                  string          comment '标题',
type                                                   int             comment '10:bug,20:建议,30:咨询 40:公告 50 消息',
cate_type                                              int             comment '11:话务系统,12:核算错误,13:常见问题',
status                                                 int             comment '状态:100:未处理,110确认中120已确认问题130正在解决140无效bug/问题150已解决200评估中210需求中220开发中230已上线240作废 250转为bug 260反馈',
close_status                                           int             comment '关闭状态',
city_id                                                int             comment '城市id',
employee_id                                            bigint          comment '员工id',
device                                                 int             comment '发生设备:1支撑后台pc 2安卓版咨询师app 3ios版咨询师app 4钉钉链接的支撑后台(m站) 5小猫头鹰宝典 6居理新房app 7 客户评价（短信链接打开 8 居理新房服务号 9 居理新房网站 10 人力系统 11 cms 12 侃侃而坛 13 招聘系统',
is_urgent                                              int             comment '紧迫度:1不紧急 2紧急',
plan_solve_date                                        int             comment '预计解决日期',
reply_num                                              bigint          comment '回复数',
last_poster                                            int             comment '最后发表人',
last_poster_time                                       int             comment '最后发表时间',
last_poster_type                                       int             comment '最后发表类型1:普通回复 2:管理员回复',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
is_delete                                              int             comment '是否删除，1未删除，2已删除',
is_notice                                              int             comment '删帖是否站内信通知楼主1:通知 2不通知',
content                                                string          comment '正文',
is_top                                                 int             comment '是否置顶 1 是 2 否',
is_elite                                               int             comment '是否加精 1 是 2 否',
follower_id                                            int             comment '当前跟进人id',
follow_end_datetime                                    int             comment '当前跟进结束时间',
pv                                                     int             comment '浏览次数',
uv                                                     int             comment '访问人数',
last_solve_datetime                                    int             comment '最后解决时间',
elite_datetime                                         int             comment '加精时间',
top_datetime                                           int             comment '置顶时间',
content_pic_sum                                        int             comment '发文图片数量',
hot_val                                                int             comment '热度值',
last_solver_id                                         int             comment '最后操作人',
bug_type                                               int             comment 'bug 类型 0 未知， 1 研发bug，2 页面适配，3 数据错误，4 产品bug，5 其他',
bug_other                                              string          comment 'bug原因，其它的补充内容',
problem_depart_id                                      int             comment '责任部门id',
check_type                                             int             comment '核算类型 1带看核算，2认购核算，3草签/网签核算',
order_id                                               string          comment '关联订单id 逗号分隔',
building_id                                            string          comment '楼盘id，逗号分隔',
relation_attr                                          int             comment '关联属性 0 无关联 1 根问题 2 子问题',
is_anonymous                                           int             comment '是否匿名 1 是 2 否',
content_type_origin                                    int             comment '原始内容分类类型',
solve_type                                             int             comment '确认解决类型 0 未知 1真正解决 2达成一致后解决',
content_type_id                                        int             comment '内容分类id',
demand_id                                              int             comment '关联产品需求id',
solution                                               string          comment '解决方案',
type_origin                                            int             comment '原始帖子类型10:bug,20:建议,30:咨询 40:公告 50 消息',
is_weekly                                              int             comment '是否是周报 1 是 2 否',
plan_solve_date_log                                    string          comment '预计解决时间（非精确值 ，精确值取plan_solve_date）',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bbs_forum'
;
