drop table if exists ods.esf_order_contact;
create external table ods.esf_order_contact(
id                                                     bigint          comment '二手房买方订单联系id',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
order_id                                               bigint          comment '二手房买方订单id',
follow_up_id                                           bigint          comment '二手房买方订单跟进id',
employee_id                                            bigint          comment '所属咨询师id',
contact_employee_id                                    bigint          comment '联系咨询师id',
contact_datetime                                       int             comment '计划/实际 联系时间',
status                                                 int             comment '1:预约2，实际',
city_id                                                int             comment '城市id',
target_type                                            int             comment '1可邀约2解决关键问题',
content                                                string          comment '联系内容',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_order_contact'
;
