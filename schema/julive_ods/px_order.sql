drop table if exists ods.px_order;
create external table ods.px_order(
id                                                     int             comment '主键 订单自增id',
order_id                                               bigint          comment '订单业务展示id',
city_id                                                int             comment '城市id(0-全国)',
class_id                                               int             comment '班级id',
director_id                                            int             comment '班主任id',
swift_id                                               int             comment '雨燕id',
charge_id                                              int             comment '主管id',
guide_id                                               int             comment '引导人id',
status                                                 int             comment '订单状态(1-已入组，2-走进居理，3-知识通关，4-知识考试通过，5-知识考试不通过，6-首电通关，7-首电考试通过，8-技能通关，9-技能考试通过，10-技能考试不通过，99-无效状态)',
is_close                                               int             comment '是否关闭(1-否,2-是)',
close_type                                             int             comment '关闭原因(1-个人规划及发展、2-薪资及待遇、3-岗位认同、4-团队管理、5-公司认同、6-工作环境、7-岗位能力、8-职业素质、9-其他 )',
close_content                                          string          comment '关闭补充说明',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/px_order'
;
