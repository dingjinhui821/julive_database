drop table if exists ods.kfs_project_follow;
create external table ods.kfs_project_follow(
id                                                     int             comment '主键id',
developer_id                                           int             comment '开发商id,对应kfs_developer_info表中的id',
developer_name                                         string          comment '开发商名称',
city_id                                                int             comment '城市id',
project_id                                             bigint          comment '楼盘id,对应cj_project表',
project_name                                           string          comment '楼盘名称',
project_type                                           bigint          comment '楼盘业态',
developer_project_relation_id                          int             comment '开发商对应楼盘关系表(kfs_developer_project_relation)的id',
flow_status                                            int             comment '流量状态,1:无意向,2:高意向,3:走合同中,4:出款中,5:已出款,6:投放中,7:投放结束',
system_status                                          int             comment '系统状态,1:无意向,2:高意向,3:走合同中,4:出款中,5:已出款,6:使用中,7:使用结束',
ad_status                                              int             comment '广告位状态,1:无意向,2:高意向,3:走合同中,4:出款中,5:已出款,6:投放中,7:投放结束',
is_delete                                              int             comment '是否删除,0:否,1:是',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/kfs_project_follow'
;
