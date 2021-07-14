drop table if exists ods.cj_license;
create external table ods.cj_license(
id                                                     int             comment '',
license_type                                           int             comment '证件类型 1销售许可证 2预售许可证',
license_number                                         string          comment '证件号',
sale_number                                            string          comment '外销证号',
issue_datetime                                         int             comment '发证日期',
valid_start_datetime                                   int             comment '有效期开始时间',
valid_end_datetime                                     int             comment '有效期结束时间',
valid_period                                           string          comment '有效期',
overall_construct_acreage                              double          comment '整体建筑面积',
overall_construct_num                                  int             comment '整体建筑套数',
overall_sale_acreage                                   double          comment '整体外销面积',
overall_sale_num                                       int             comment '整体外销套数',
is_show                                                int             comment '是否显示 1显示 2不显示',
get_way                                                int             comment '获取途径 1渠道拓展，2渠道商务，3咨询师，4竞品，5销售，6售楼处，7其他',
competor                                               string          comment '竞品名称',
others                                                 string          comment '其他',
note                                                   string          comment '备注',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
city_id                                                int             comment '',
relation_license_buildings                             string          comment '关联证件楼栋id，逗号分隔',
relation_project_ids                                   string          comment '关联楼盘id，逗号分隔',
relation_building_ids                                  string          comment '关联证件楼栋id，逗号分隔',
data_from                                              int             comment '数据来源 1搜房楼盘灌入 2安居客楼盘灌入 3搜狐焦点楼盘灌入 10运营添加 11渠道添加',
license_number_arab                                    bigint          comment '证件号-数字',
district_id                                            int             comment '区域id',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_license'
;
