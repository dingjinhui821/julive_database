drop table if exists ods.yw_wechat_list;
create external table ods.yw_wechat_list(
id                                                     int             comment '主键id',
city_id                                                int             comment '城市id',
employee_id                                            int             comment '咨询师id',
employee_leader_id                                     int             comment '咨询师主管id',
employee_manager_id                                    int             comment '咨询师经理id',
employee_wx_id                                         string          comment '咨询师微信id',
friend_count                                           string          comment '好友统计 json串{‘1’:’80’,’2’:’66’}',
is_finish_group                                        int             comment '是否完成分组（1.已完成）',
is_finish_bind                                         int             comment '是否完成绑定',
group_ratio                                            double          comment '分组比率',
finish_ratio                                           double          comment '绑定比率',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_wechat_list'
;
