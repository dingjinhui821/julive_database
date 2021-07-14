drop table if exists ods.ex_sign_approval_project;
create external table ods.ex_sign_approval_project(
id                                                     int             comment '',
ex_approval_id                                         int             comment '审批id',
project_id                                             int             comment '楼盘id',
contract_id                                            int             comment '结佣合同id',
cooperation_status                                     int             comment '合作状态 1合作 2外联',
time_type                                              int             comment '变更时间类型 1达到带看条件 2达到带看条件，且到达指定时间',
effective_time                                         int             comment '指定时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_sign_approval_project'
;
