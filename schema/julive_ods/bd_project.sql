drop table if exists ods.bd_project;
create external table ods.bd_project(
project_id                                             bigint          comment '',
project_name                                           string          comment '楼盘名称',
group_project_id                                       bigint          comment '聚合id',
project_price                                          double          comment '楼盘价格',
project_address                                        string          comment '楼盘地址',
project_url                                            string          comment '楼盘url链接',
project_district                                       string          comment '楼盘区域',
district_id                                            int             comment '区域id',
source                                                 int             comment '来源',
business_layout                                        string          comment '业态类型',
project_type                                           int             comment '侃家业态',
status                                                 int             comment '抓取状态',
city_id                                                int             comment '城市id',
alias                                                  string          comment '别名',
source_project_id                                      string          comment '楼盘源id,源网站楼盘id,有些无数据',
total_price                                            string          comment '总价',
project_tag                                            string          comment '楼盘标签，销售状态的标签，在售，未售，售罄',
parent_project_id                                      bigint          comment '生成源楼盘id，业态拆分时，记录当前楼盘是由那个楼盘生成过来的',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
is_group_source                                        int             comment '是否是生成源:0不是1是，表示聚合楼盘是从此源的这个楼盘开始的',
sell_out                                               string          comment '售罄',
old_project_id                                         bigint          comment '已废弃，旧id',
img_tag                                                int             comment '1有图片2无图片',
second_price                                           string          comment '多业态对应的多价格',
match_type                                             int             comment '匹配类型 1:精确匹配 2:模糊匹配 3:人工匹配',
total_price_json                                       string          comment '多业态对应的多总价',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
ring_road                                              string          comment '环线',
trade_area                                             string          comment '区域板块',
live_date                                              int             comment '(已废弃)交房时间',
property_right                                         string          comment '产权年限',
far                                                    string          comment '容积率',
greening                                               string          comment '绿化率',
manage_fee                                             string          comment '物业费',
building_type                                          string          comment '建筑类型',
developer                                              string          comment '开发商',
car_space                                              string          comment '车位比',
car_num                                                string          comment '车位数',
property_services                                      string          comment '物业公司',
building_info                                          string          comment '楼栋信息',
water_electricity                                      string          comment '水电气',
current_open_time                                      bigint          comment '当期开盘时间',
lat                                                    string          comment '纬度值',
lng                                                    string          comment '经度值',
cj_project_id                                          bigint          comment 'comjia楼盘id',
is_new                                                 int             comment '是否是新抓:0否1是',
is_delete                                              int             comment '是否删除(已废弃) 1:未删除 2:已删除',
is_deal                                                int             comment '是否处理，0未处理，1 拆分已处理，2 灌入已处理',
project_price_text                                     string          comment '楼盘报价(不作处理的文本)',
project_price_text_json                                string          comment '各业态楼盘报价(不作处理的文本)',
project_tag_update_datetime                            int             comment '销售状态更新时间',
project_type_text                                      string          comment '业态文本',
pay_info                                               string          comment '折扣描述',
construction_stage                                     int             comment '建设阶段 1现房 2期房',
tags                                                   string          comment '楼盘便签',
volume_type                                            int             comment '(已废弃)放量类型 1.已开盘 3.预计开盘',
live_date_type                                         int             comment '(已废弃)交房时间类型 1预计交房时间 2实际交房时间',
is_become_cj_project                                   int             comment '是否可以新增为居理新楼盘 1.是, 2.否, 3.处理中 4处理完成 5无此业态',
business_layout_is_change                              int             comment '抓取的业态文本是否有变化 1有 2无（新business_layout跟之前比是否有变化）',
volume_info_json                                       string          comment '(已废弃)放量信息json',
is_available                                           int             comment '楼盘是否可用 1是(未下架) 2否(已下架)',
last_catch_datetime                                    int             comment '最新一次抓取时间',
last_catch_version                                     int             comment '最新一次抓取版本',
brand_developer                                        string          comment '品牌开发商',
decorate_info                                          string          comment '装修情况',
decorate_fee                                           double          comment '装修费用',
heating                                                string          comment '供暖方式',
price_description                                      string          comment '价格说明',
building_img_url                                       string          comment '楼作图源地址',
building_img_path                                      string          comment '楼盘图下载后的地址',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/bd_project'
;
