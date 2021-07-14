drop table if exists ods.ex_meet;
create external table ods.ex_meet(
id                                                     int             comment '',
ex_order_id                                            int             comment 'bd单id',
plan_datetime                                          int             comment '预约见面时间',
real_datetime                                          int             comment '实际见面时间',
status                                                 int             comment '状态 1预约 2实际',
follow_employee_id                                     int             comment '跟进bd人',
ex_order_status                                        int             comment 'bd单状态',
expected_terms_result                                  int             comment '预计是否可以达成初步合作条款 1可以 2不可以',
terms_result                                           int             comment '是否有初步合作条款 1有 2无',
note                                                   string          comment '见面详情',
meet_address                                           string          comment '见面地点',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
old_meet_id                                            int             comment '历史见面id，对应yw_cooperate_follow表的id',
ex_sign_record_json                                    string          comment '签约单修改记录json',
is_add_sign                                            int             comment '是否新增签约单 1是 2否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_meet'
;
