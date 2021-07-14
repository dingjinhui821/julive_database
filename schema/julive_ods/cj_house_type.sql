drop table if exists ods.cj_house_type;
create external table ods.cj_house_type(
house_type_id                                          bigint          comment '户型id',
app_house_type_id                                      int             comment '咨询师编辑户型id',
house_type_num                                         string          comment '户型编号（业务编号）',
room_type                                              int             comment '厅室类别:0不限, 1一居, 2二居, 3三居, 4四居, 5五居及以上, 6 loft',
summary                                                string          comment '厅室描述',
acreage                                                int             comment '建筑面积',
price                                                  double          comment '总价',
down_pay                                               double          comment '首付',
month_pay                                              double          comment '月供',
total_count                                            int             comment '总套数',
surplus_count                                          int             comment '剩余套数',
advantage                                              string          comment '户型优势(已作废)',
defects                                                string          comment '户型劣势(已作废)',
project_id                                             bigint          comment '楼盘id',
apart_img                                              string          comment '户型图',
trans_price                                            double          comment '成交单价',
offer_price                                            double          comment '报价单价',
show_index                                             int             comment '排序',
ac_acreage                                             string          comment '套内面积',
rate                                                   double          comment '平均折扣',
is_estimate                                            int             comment '是否估算，1: 否  2:是',
status                                                 int             comment '状态，1: 未售  2:在售  3:售罄 4:待售',
floor_height                                           double          comment '层高',
project_type                                           bigint          comment '项目类型 1 => 住宅, 2 => 别墅, 3 => 商业,55 => 商铺, 58 => 写字楼',
create_datetime                                        bigint          comment '创建时间',
update_datetime                                        bigint          comment '更新时间',
is_show_room_type                                      int             comment '是否展现厅室类别，1: 否  2:是',
employee_id                                            bigint          comment '咨询师id',
employee_name                                          string          comment '咨询师姓名',
orientation                                            string          comment '房屋朝向',
good_desc                                              string          comment '优势描述',
bad_desc                                               string          comment '劣势描述',
click_num                                              bigint          comment '点击数量',
is_loft                                                int             comment '是否loft:1是，2否',
building_type                                          int             comment '建筑类型:0不限,1独栋,2双拼,3联排,4叠拼,5平层,6复式,7洋房,8loft,9非loft',
house_tag                                              string          comment '户型标签',
forecast                                               int             comment '是否预估 1是 2否',
small_family                                           int             comment '是否是小户型 1:是2:否,面积在0-80平,脚本自动计算',
low_price                                              int             comment '是否是低总价 1:是2:否,价格在0-2百万,脚本自动计算',
bd_house_type_id                                       int             comment '抓取户型id，灌入专用',
layout_shi                                             int             comment '几室',
layout_ting                                            int             comment '几厅',
layout_chu                                             int             comment '几厨',
layout_wei                                             int             comment '几卫',
master_bed_room                                        string          comment '主卧居室详解',
toilet                                                 string          comment '卫生间居室详解',
living_room                                            string          comment '客厅详解',
kitchen                                                string          comment '厨房详解',
restaurant                                             string          comment '餐厅详解',
terrace                                                string          comment '露台详解',
house_on_sale_num                                      int             comment '户型在售余量',
house_on_sale_tag                                      int             comment '剩余房源标签:1房源充足,2仅剩顶底层,3仅剩顶层,4仅剩底层,5少于10套',
house_on_sale_summary                                  string          comment '剩余房源说明',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
relation_buildings                                     string          comment '关联楼栋id，逗号分隔',
is_false                                               int             comment '是否是假户型 1:是 2否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_house_type'
;
