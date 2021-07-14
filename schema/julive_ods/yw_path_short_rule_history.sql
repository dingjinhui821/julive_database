drop table if exists ods.yw_path_short_rule_history;
create external table ods.yw_path_short_rule_history(
id                                                     int             comment '主键',
rule_id                                                int             comment '配置表id',
distribute_type                                        int             comment '分配类型（1-路径缩短 2.优选客户）',
city_id                                                int             comment '城市id',
pc_m_port                                              string          comment 'pc/m留点口',
app_port                                               string          comment 'app留点口',
is_merit                                               int             comment '是否择优:1是2否',
alloc_channel                                          string          comment '分配渠道',
alloc_obj                                              int             comment '分配对象1:所有2按排名3:按组4:按个人',
alloc_value                                            string          comment '分配值',
ab_test                                                int             comment '是否进行ab测试 1:是 2:否',
alloc_rule                                             int             comment '分配规则1平均分配2与订单分配规则页中配置的分配规则一致',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
is_delete                                              int             comment '1:未删除2:已删除',
little_program_port                                    string          comment '小程序分配留电口',
equipment                                              string          comment '设备(1.居理新房pc站2.居理新房m站3.客服4.支撑系统10.二手房pc站,20.二手房m站,101.居理新房android,201.居理新房ios,102.居理咨询师android,301.居理新房小程序,302.居理新房小程序（城市版）,303.居理新房小程序（楼盘版）401.百度小程序)',
is_preferred_customers                                 int             comment '是否显示优选客户标识（1.是 2.否）',
is_open                                                int             comment '是否开启（1.开启 2.关闭）',
alloc_percentage                                       int             comment 'a组分配比例（开启ab测试生效）',
start_datetime                                         int             comment '规则开始时间',
preferred_pass_rate                                    int             comment '优选通过比例',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_path_short_rule_history'
;
