drop table if exists ods.esf_house_spider;
create external table ods.esf_house_spider(
house_spider_id                                        bigint          comment '',
source                                                 int             comment '来源',
url                                                    string          comment '详情页url',
url_md5                                                string          comment '详情页url_md5',
city_id                                                int             comment '城市id',
district                                               string          comment '区域',
district_source_id                                     string          comment '抓取源，区域id',
title                                                  string          comment '标题',
total_price                                            string          comment '总价',
house_price                                            string          comment '单价',
room_info                                              string          comment '户型',
acreage                                                double          comment '面积',
village_name                                           string          comment '小区名',
village_source_id                                      bigint          comment '源网站id',
floor                                                  string          comment '所在楼层',
floor_total                                            string          comment '总楼层',
orientation                                            string          comment '朝向',
renovation                                             string          comment '装修情况',
house_type                                             string          comment '房屋用途',
building_type                                          string          comment '建筑类型',
building_year                                          string          comment '建筑年代',
trade_area                                             string          comment '商圈',
see_time                                               string          comment '看房时间',
subway                                                 string          comment '地铁',
status                                                 int             comment '状态:1未抓取，2已抓取，3已处理，4删除',
img_tag                                                int             comment '1有图片 2无图片',
creator                                                bigint          comment '创建者',
updator                                                bigint          comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
property                                               string          comment '产权年限',
listing                                                string          comment '挂牌时间',
last_exchange                                          string          comment '上次交易时间',
elevator                                               string          comment '配备电梯',
room_count                                             int             comment '房间数量',
room_type                                              int             comment '户型，最大值5',
tags                                                   string          comment '贝壳抓取过来标签',
village_spider_id                                      int             comment '抓取小区id',
village_url                                            string          comment '小区详情页地址',
village_url_md5                                        string          comment '小区详情页url_md5',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/esf_house_spider'
;
