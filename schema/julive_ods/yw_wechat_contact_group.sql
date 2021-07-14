drop table if exists ods.yw_wechat_contact_group;
create external table ods.yw_wechat_contact_group(
id                                                     int             comment '主键id',
group_type                                             int             comment '分组(1.客户，2.同事，3.销售，4.其他，5.待分组)',
wx_id                                                  string          comment '微信号',
employee_wx_id                                         string          comment '咨询师微信id',
employee_id                                            int             comment '咨询师id',
city_id                                                int             comment '城市id',
is_bind                                                int             comment '是否绑定（1-未绑定 2-已绑定）',
is_remind                                              int             comment '是否提醒(1.是,2.否)',
pre_group_type                                         int             comment '待分组中（1.疑似销售）',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
talk_time                                              int             comment '最新聊天时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_wechat_contact_group'
;
