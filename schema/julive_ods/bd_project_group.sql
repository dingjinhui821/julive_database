drop table if exists ods.bd_project_group;
create external table ods.bd_project_group(
id                                                     bigint          comment '聚合id',
bd_project_id                                          bigint          comment '楼盘id,表示以此楼盘做的聚合生成的楼盘组,此id是根据source来定，如果source是99,表示cj_project的id,否则表示bd_project的id',
project_name                                           string          comment '楼盘名称',
alias                                                  string          comment '楼盘别名',
project_type                                           int             comment '业态类型:取值范围和侃家的值是一致的',
source                                                 int             comment '源,bd_project_id对应的楼盘源',
dt_state                                               int             comment '动态的审核状态:1,未审核,2部分审核,3已审核',
state                                                  int             comment '总审核状态 1:待审核 2:部分待审核 3:待定 4:已审核',
city_id                                                int             comment '城市id',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
is_show                                                int             comment '是否下架,0未下架,1已下架',
step                                                   int             comment '聚合步骤',
pic_audit_status                                       int             comment '图片审核结果:1未审核，2部分审核，3全部审核',
project_address                                        string          comment '楼盘地址',
is_cooperate                                           int             comment '是否是合作楼盘，1:是  2:否',
district_id                                            bigint          comment '区域id:值范围和侃家的区域id一致',
state_source                                           int             comment '审核状态 1:未审核  2:待定  3:已审核',
creator                                                bigint          comment '创建者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_project_group'
;
