drop table if exists ods.yw_department_architecture;
create external table ods.yw_department_architecture(
id                                                     bigint          comment '部门结构id',
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
cate_id                                                int             comment '部分分类，1大区 2城市公司 3中心 4部门 5组',
description                                            string          comment '部门描述',
level                                                  int             comment '级别',
updator                                                int             comment '更新人',
type                                                   int             comment '部门标志位 1咨询组，2咨询部，3渠道部，4售前咨询、0其他',
path                                                   string          comment '分类路径',
department_attributes                                  int             comment '1 支撑部门 2业务部门 3 职能部门',
effective_date                                         int             comment '生效时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_department_architecture'
;
