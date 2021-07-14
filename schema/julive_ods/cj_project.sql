drop table if exists ods.cj_project;
create external table ods.cj_project(
project_id                                             bigint          comment '楼盘id',
is_importent_notice                                    int             comment '是否重点通知，1是2否',
see_project_note                                       string          comment '带看注意事项',
report_see_project_note                                string          comment '报备带看注意事项',
project_num                                            string          comment '楼盘编号',
name                                                   string          comment '楼盘名称',
summary                                                string          comment '楼盘概述',
group_project_id                                       bigint          comment '楼盘聚合id',
tag_bak                                                string          comment '旧标签tag改成备份tag_bak',
property_right                                         string          comment '产权年限(已废弃)',
live_date                                              int             comment '交房时间(已废弃)',
decorate                                               string          comment '装修情况(已废弃)',
decorate_money                                         double          comment '装修费用(已废弃)',
far                                                    string          comment '容积率',
heating                                                string          comment '供暖方式',
car_space                                              string          comment '车位比',
greening                                               string          comment '绿化率',
manage_fee                                             string          comment '物业费(已废弃)',
water_electricity                                      string          comment '水电气(已废弃)',
business_layout                                        string          comment '建筑类型(已废弃)',
developer                                              string          comment '开发商',
property_services                                      string          comment '物业服务(已废弃)',
build_operation                                        string          comment '建筑施工',
landscaping                                            string          comment '园林绿化',
quality_label                                          string          comment '品质标签',
current_open_time                                      bigint          comment '当期开盘时间(已废弃)',
current_sets                                           string          comment '当期套数',
next_open_time                                         bigint          comment '下次开盘时间(已废弃)',
next_sets                                              string          comment '下期套数',
full_pay_rate                                          double          comment '全款折扣',
loan_pay_rate                                          double          comment '贷款折扣',
last_month_rate                                        double          comment '上月平均折扣',
current_rate                                           double          comment '当期报价',
last_month_deal_price                                  double          comment '上月成交价格',
address                                                string          comment '位置描述',
ring_road                                              double          comment '环线',
near_distance                                          string          comment '近环距离',
far_distance                                           string          comment '远环距离',
admin_id                                               int             comment '创建人',
submit_time                                            bigint          comment '创建时间',
index_img                                              string          comment '首页图片',
quality_img                                            string          comment '品质图片',
coordinate                                             string          comment '楼盘百度坐标',
enstate                                                string          comment '环境现状',
en_plan_img                                            string          comment '环境规划图',
en_plan_text                                           string          comment '环境规划描述',
en_plan_doc                                            string          comment '环境规划相关文件',
en_air_num                                             double          comment '环境空气数值(已废弃)',
en_voice_num                                           double          comment '环境噪音数值(已废弃)',
en_bad                                                 string          comment '环境不利因素',
en_bad_doc                                             string          comment '环境不利因素相关文件',
ambitus_text                                           string          comment '周边描述',
district_id                                            bigint          comment '区域id',
district_name                                          string          comment '区域名称',
project_type                                           bigint          comment '业态',
acreage_min                                            double          comment '最小面积',
acreage_max                                            double          comment '最大面积',
price_min                                              double          comment '最小总价',
price_max                                              double          comment '最大总价',
phone                                                  string          comment '联系电话',
enroll_num                                             int             comment '报名人数',
school_district_room                                   int             comment '学区房',
near_subway                                            int             comment '临地铁',
near_subway_list                                       string          comment '地铁线列表',
good_decorate                                          int             comment '精装修',
success_rate                                           double          comment '组团成功率',
is_estimate                                            int             comment '是否估算，1: 否  2:是',
status                                                 int             comment '状态，1: 未售  2:在售  3:售罄 4:待售',
alias                                                  string          comment '别名',
school_district_room_list                              string          comment '学区房列表',
near_subway_station_list                               string          comment '地铁站列表',
is_cooperate                                           int             comment '是否是合作楼盘，1:是  2:否',
sale_num                                               int             comment '侃家楼盘出售数量',
update_datetime                                        bigint          comment '更新时间',
create_datetime                                        bigint          comment '创建时间',
pay_info                                               string          comment '折扣信息',
is_discount                                            int             comment '是否折扣:1是，2否根据pay_info来判断，有值为1，无值为2',
updator                                                bigint          comment '楼盘维护咨询师',
click_num                                              bigint          comment '点击量',
error_alias                                            string          comment '楼盘错别字别名',
is_outreach                                            int             comment '是否外联:0不是，1是',
recent_instructions                                    string          comment '楼盘近况通知',
lat_lng                                                string          comment '售楼处经纬度',
lng                                                    string          comment '楼盘经度',
lat                                                    string          comment '楼盘纬度',
trade_area                                             bigint          comment '区域板块id',
is_loft                                                int             comment '是否loft:1是，2否',
card_timing                                            int             comment '排卡开始时间',
competitive_project                                    string          comment '竞品楼盘',
ec_flow                                                int             comment '是否刷电商,1是,2否',
during_sale_period                                     int             comment '在售期数',
take_land_old                                          int             comment '拿地时间(已废弃)',
take_land                                              string          comment '拿地时间 2017年 2017年2月 2017年2月8日',
is_park_building                                       int             comment '是否公园地产，1是，2否',
is_personal_steward                                    int             comment '是否私人管家，1是，2否',
tag                                                    string          comment '替换标签tag_replace改成备份tag',
competitive_project_name                               string          comment '竞品楼盘名称',
architectural_style                                    string          comment '建筑风格',
elite_plate                                            string          comment '高端板块',
building_info                                          string          comment '楼栋信息',
report_see_project_img                                 string          comment '报备图片',
recent_instructions_img                                string          comment '楼盘近况图片',
forecast                                               int             comment '是否预估 1是 2否',
diy_icon                                               string          comment '自定义图标',
seo_title                                              string          comment 'seo tdk中的头',
seo_description                                        string          comment 'seo tdk中的描述',
seo_keywords                                           string          comment 'seotdk中的关键字',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
is_show                                                bigint          comment '楼盘是否显示 1显示 2隐藏',
same_price_project_ids                                 string          comment '同价位推荐楼盘id,多个id用逗号分隔',
sign_num                                               int             comment '最近90天，楼盘签约统计',
see_num                                                int             comment '最近90天，楼盘带看量统计',
operation_score                                        int             comment '运营数据排序得分',
is_group_source                                        int             comment '',
latest_history_price                                   double          comment '最新历史价格',
latest_change_time                                     bigint          comment '最新修改当期报价时间',
diy_text                                               string          comment '自定义文案',
diy_show_start_date                                    bigint          comment '自定义文案显示开始时间',
diy_show_end_date                                      bigint          comment '自定义文案显示结束时间',
special_field_cfg                                      int             comment '是否个性化显示字段信息（默认0非个性化，1为个性化展示）',
is_have_house_type                                     int             comment '是否有户型:1无2有 ',
payback_employee_id                                    int             comment '回款负责人id',
remark                                                 string          comment '带看备注',
band_status                                            int             comment '带看状态:1非合作2正常带看3暂停带看',
cooperate_remark                                       string          comment '合作备注',
general_score                                          int             comment '冗余 推荐楼盘 主推楼盘 竞品楼盘排序',
district_general_score                                 int             comment '区域主推楼盘得分',
payback_employee_mobile                                string          comment '回款负责人联系方式',
panorama_code                                          string          comment '全景看房资源id',
current_price_type                                     int             comment '单价类型',
latest_history_price_type                              int             comment '最近历史报价类型',
price_desc                                             string          comment '价格说明',
price_type                                             int             comment '总价类型',
plate_longitude                                        string          comment '板块经度',
plate_latitudes                                        string          comment '板块纬度',
building_img                                           string          comment '楼栋图片',
building_img_size                                      string          comment '楼栋图片尺寸',
project_introduce                                      string          comment '楼盘介绍json',
old_pay_info                                           string          comment '往期优惠信息',
is_old_discount                                        int             comment '是否折扣:1是，2否根据old_pay_info来判断，有值为1，无值为2',
live_date_year                                         int             comment '交房时间年份(已废弃)',
live_date_month                                        int             comment '交房时间月份(已废弃)',
live_date_day                                          int             comment '交房时间日(已废弃)',
live_date_ten                                          int             comment '交房时间旬 1上旬 2中旬 3下旬(已废弃)',
open_time_year                                         int             comment '开盘时间年份',
open_time_month                                        int             comment '开盘时间月份',
open_time_day                                          int             comment '开盘时间日',
open_time_ten                                          int             comment '开盘时间旬 1上旬 2中旬 3下旬',
special_price_house_desc                               string          comment '特价房描述',
is_special_price_house                                 int             comment '是否是特价房，1:是，2:否',
brand_developer                                        string          comment '品牌开发商，逗号分隔',
on_sale_num                                            int             comment '在售房源套数',
push_project_num                                       int             comment '加推房源套数',
push_project_year                                      int             comment '加推楼盘时间年份',
push_project_month                                     int             comment '加推楼盘时间月份',
push_project_day                                       int             comment '加推楼盘时间日',
push_project_ten                                       int             comment '加推楼盘时间旬',
order_num                                              int             comment '最近90天，楼盘订单量（咨询人数/关注人数）',
is_have_model_room                                     int             comment '是否有样板间 1是 2否',
take_land_price                                        double          comment '拿地总价',
is_continue_push                                       int             comment '是否会持续加推 1是 2否',
creator                                                int             comment '创建人',
is_folder_special                                      int             comment '是否折叠特价房，1:是，2:否',
abtest_general_score                                   int             comment '测试楼盘得分',
match_type                                             int             comment '匹配类型 1:精确匹配 2:模糊匹配 3:人工匹配',
data_from                                              int             comment '数据来源 1搜房楼盘灌入 2安居客楼盘灌入 3搜狐焦点楼盘灌入 10运营添加 11渠道添加',
project_desc                                           string          comment '楼盘概述',
volume_status                                          int             comment '楼盘放量状态 1:新开楼盘 2:即将开盘',
pay_title                                              string          comment '折扣描述标题',
project_video_num                                      int             comment '楼盘显示的视频数量',
volume_date_pre                                        int             comment '预计开盘、加推时间',
volume_date_done                                       int             comment '已开盘、加推时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project'
;