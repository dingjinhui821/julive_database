drop table if exists ods.yw_quality_testing_server;
create external table ods.yw_quality_testing_server(
id                                                     int             comment '',
create_datetime                                        int             comment '创建时间',
creator                                                bigint          comment '创建人',
update_datetime                                        int             comment '更新时间',
updator                                                bigint          comment '更新人',
order_id                                               bigint          comment '订单id',
city_id                                                bigint          comment '城市id',
city_name                                              string          comment '城市名称',
user_id                                                bigint          comment '用户id',
user_mobile                                            string          comment '来电号码',
distribute_datetime                                    int             comment '分配时间',
order_creator                                          bigint          comment '订单创建者',
order_creator_name                                     string          comment '订单创建者',
is_distribute                                          int             comment '是否分配，及不分配的原因:1、分配 2、谈合作3、超区域4、不关注楼盘5、必须找售楼处6、关注二手房7、之前已上户8、不愿留电话9、电话无法接通99、其他',
is_distribute_result                                   string          comment '是否分配咨询师，及不分配原因',
distribute_cause                                       string          comment '订单原因分类',
server_quality_result                                  string          comment '客服质检结果',
server_quality_score                                   int             comment '客服质检分数',
clue_quality_result                                    string          comment '线索质检结果',
clue_quality_score                                     int             comment '线索质检分数',
service_note                                           string          comment '服满备注',
quality_employee_id                                    bigint          comment '质检人id',
quality_employee                                       string          comment '质检人',
quality_datetime                                       int             comment '质检时间',
re_quality_result                                      string          comment '复检结果',
re_quality_employee_id                                 bigint          comment '复检人id',
re_quality_employee                                    string          comment '复检人',
re_quality_datetime                                    int             comment '复检时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_quality_testing_server'
;
