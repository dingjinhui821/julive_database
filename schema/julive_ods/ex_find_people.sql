drop table if exists ods.ex_find_people;
create external table ods.ex_find_people(
id                                                     int             comment '寻人id',
ex_order_id                                            int             comment 'bd单id',
action                                                 int             comment '寻人动作 1网上检索，2友介，3陌拜案场&总部，4参加会议，5其余途径',
status                                                 int             comment '是否找到决策人 1是 2否',
decision_employee_id                                   int             comment '决策人id',
other_note                                             string          comment '其他路径描述',
ex_order_status                                        int             comment 'bd单状态',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
require_complete_datetime                              int             comment '系统固定完成寻人时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_find_people'
;
