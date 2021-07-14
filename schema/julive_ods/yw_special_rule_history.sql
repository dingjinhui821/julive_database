drop table if exists ods.yw_special_rule_history;
create external table ods.yw_special_rule_history(
id                                                     int             comment '',
special_rule_id                                        int             comment '特殊线索表id',
group_name                                             string          comment '组名',
priority                                               int             comment '优先级',
`condition`                                            string          comment '分配条件',
alloc_value                                            string          comment '分配对象对应的值',
is_normal_shhu                                         int             comment '分配对象是否同时享受正常分配规则”选项。选“是”，则该特殊分配规则下所有咨询师，可以接正常分配上户选“否”，则该特殊分配规则下咨询师只能接特殊上户',
alloc_object                                           int             comment '分配对象1:咨询师2咨询师组',
city_id                                                int             comment '城市id',
group_employee_hour_limit                              int             comment '该组咨询师每小时最多可分配数量限制',
group_employee_day_limit                               int             comment '该组咨询师每天最多可分配订单数量',
creator                                                int             comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                int             comment '更新人',
update_datetime                                        int             comment '更新时间',
enable                                                 int             comment '启用/暂停 默认为1启用 2为暂停',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_special_rule_history'
;
