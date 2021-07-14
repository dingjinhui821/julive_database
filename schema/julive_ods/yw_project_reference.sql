drop table if exists ods.yw_project_reference;
create external table ods.yw_project_reference(
id                                                     int             comment '',
project_id                                             bigint          comment '楼盘id',
bd_order_id                                            int             comment 'bd单id',
check_status                                           int             comment '核查状态:1，待核查，2，已核查',
project_relevant_info                                  string          comment '楼盘相关资料（电子+纸质）',
developer                                              string          comment '开发商',
business_layout                                        string          comment '产品业态',
average_price                                          string          comment '均价',
total_price                                            string          comment '总价',
is_have_gas                                            string          comment '有无燃气',
floor_height                                           string          comment '层高',
sale_status                                            string          comment '销售状态',
note                                                   string          comment '备注',
house_type_and_area                                    string          comment '户型+面积',
on_sale_building                                       string          comment '在售楼栋',
discount_buy                                           string          comment '团购优惠',
discount_project                                       string          comment '项目折扣',
discount_other                                         string          comment '其它折扣',
addr_project                                           string          comment '楼盘地址',
addr_sale_building                                     string          comment '售楼处位置',
creator                                                bigint          comment '创建者',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '修改者',
update_datetime                                        int             comment '修改时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_project_reference'
;
