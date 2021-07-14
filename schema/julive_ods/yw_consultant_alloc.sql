drop table if exists ods.yw_consultant_alloc;
create external table ods.yw_consultant_alloc(
employee_id                                            bigint          comment '咨询师id',
update_datetime                                        int             comment '更新时间',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '更新者',
creator                                                bigint          comment '创建者',
can_alloc                                              int             comment '咨询师自己控制是否可以分配:1可以，0不可以',
status                                                 int             comment '管理者配置咨询师状态:2还不能接待客户，1能够接待客户',
init_weight                                            int             comment '新咨询师分配用户的起步权重:100为标准平均值，小于100低于平均值，大于100高于平均值',
init_alloc_num                                         int             comment '新咨询师分配用户的起步计算数目，不能人为设置，由系统计算:值越大分配到客户的几率越小',
real_alloc_num                                         int             comment '咨询师当前实际已经分配的用户数',
rank_alloc_num                                         double          comment '用来排序的咨询师分配用户数',
fighting                                               double          comment '战斗力',
new_rank_alloc_num                                     double          comment '[新]用来排序的咨询师分配用户数',
initial_value                                          int             comment '初始值[服管设定参数]',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
alloc_sort                                             int             comment 'rank咨询师分配排名',
sys_flag                                               string          comment '当系统检测到咨询师没有通过考试后会自动立即关闭上户0不关，1关',
cause_type                                             int             comment '关上户原因',
cause                                                  string          comment '关上户原因',
is_online_see_project                                  int             comment '是否可以线上看房 1.可以 2.不可以',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_consultant_alloc'
;
