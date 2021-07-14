drop table if exists ods.yw_department_architecture_history;
create external table ods.yw_department_architecture_history(
id                                                     bigint          comment 'id',
department_id                                          bigint          comment '部门id',
pid                                                    bigint          comment '父id',
team_name                                              string          comment '组名称',
team_leader_id                                         bigint          comment '组长id',
team_leader_name                                       string          comment '组长名',
status                                                 int             comment '1启用 2禁用',
team_employee_id                                       string          comment '组员id,用,分割',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
update_datetime                                        int             comment '更新时间',
path                                                   string          comment '分类路径',
cate_id                                                int             comment '部分分类，1大区 2城市公司 3中心 4部门 5组',
description                                            string          comment '部门描述',
level                                                  int             comment '级别',
updator                                                int             comment '更新人',
type                                                   int             comment '部门类型，1咨询部',
operation_type                                         int             comment '1 默认 2移动 3删除',
effective_date                                         int             comment '生效时间',
dead_date                                              int             comment '失效时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_department_architecture_history'
;
