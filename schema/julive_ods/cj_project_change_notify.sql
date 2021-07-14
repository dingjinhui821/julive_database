drop table if exists ods.cj_project_change_notify;
create external table ods.cj_project_change_notify(
id                                                     bigint          comment '',
project_id                                             bigint          comment '楼盘id',
name                                                   string          comment '楼盘名称',
city_id                                                int             comment '城市id',
tags                                                   string          comment '标签（常量），多个以逗号隔开',
change_notify                                          string          comment '变更通知',
imgs                                                   string          comment '图片，多张以逗号隔开',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
remark                                                 string          comment '带看备注',
band_status                                            int             comment '带看状态:1非合作2正常带看3暂停带看',
cooperate_remark                                       string          comment '合作备注',
is_cooperate                                           int             comment '是否合作:1是2否',
is_delete                                              int             comment '是否删除变更通知，1:否  2:是',
recent_take_time_min                                   int             comment '近期可带看时间',
recent_take_time_max                                   int             comment '近期可带看时间',
quantity_description                                   string          comment '',
business_type                                          int             comment '业务类型',
business_id                                            bigint          comment '业务id',
attachments                                            string          comment '附件，多个用逗号分隔',
operation_audit_id                                     bigint          comment '审核记录id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_change_notify'
;
