drop table if exists ods.hj_experiment_main;
create external table ods.hj_experiment_main(
id                                                     int             comment '主键id',
name                                                   string          comment '实验名称',
hypothesis                                             string          comment '实验假设',
plan                                                   string          comment '实验计划',
priority                                               int             comment '实验优先级 1 p0重要且紧急 2p1不重要但紧急 3p2重要但紧急,4p3不重要且不紧急',
employee_ids                                           string          comment '实验负责人，用逗号分隔',
operation_object                                       int             comment '实验操作对象 1账户 2计划 3单元 4关键字 5创意',
adjust_variant                                         int             comment '实验调整的变量 7:修改账户推广地域, 8:修改账户日预算, 9:修改账户推广时段, 10:修改账户出价系数, 11:修改账户投放人群, 12:账户ocpc设置, 13:修改账户落地页, 14:搜索意图定位, 15:目标客户追投, 16:修改计划日预算, 17:修改计划推广地域, 18:修改计划推广时段, 19:计划ocpc设置, 20:修改计划落地页, 14:搜索意图定位, 21:修改动态创意, 22:新增/删除计划, 23:暂停/启动计划, 24:计划层级否定关键词, 25:修改计划移动/计算机出价比例, 26:修改计划展现方式, 27:修改单元推广地域, 28:修改单元推广时段, 29:修改单元落地页, 30:新增/删除单元, 31:暂停/启动单元, 32:单元层级否定关键词, 33:修改单元移动/计算机出价比例, 34:修改单元出价, 35:修改单元匹配方式, 36:修改单元分匹配模式出价, 37:修改关键词落地页, 38:新增/删除关键词, 39:暂停/启动关键词, 40:修改关键词出价, 41:修改关键词匹配方式, 43:新增/删除创意, 44:暂停/启动创意, 45:编辑创意, 46:修改创意设备偏好, 47:修改创意落地页',
attention_target                                       string          comment '实验关注指标 1.展示,2.点击,3.消费,4.ctr,5.cpc,6.cvr,7.线索量,8.线索成本,9.上户量,10.上户成本,11.上户率,12.带看量,13.认购量,14.上认率,15.线认率,16.跳出率',
cover_rate                                             double          comment '整个实验的操作覆盖占比',
experiment_status                                      int             comment '实验状态:0草稿, 1校验中，2创建失败，3实验中,4实验结束',
status_reason                                          string          comment '修改实验状态时的原因说明',
result_flag                                            int             comment '标识实验结果:1有明确正向结论,2有明确负向结论,3结论暂不明确',
operation_begin_datetime                               int             comment '账户操作开始时间',
operation_end_datetime                                 int             comment '账户操作结束时间',
group_regulation                                       string          comment '实验分组规则',
start_date                                             int             comment '实验开始时间',
end_date                                               int             comment '实验结束时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
keyword_num                                            int             comment '关键词个数',
source                                                 int             comment '实验类型 1手动创建实验 2自动创建实验',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/hj_experiment_main'
;
