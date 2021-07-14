drop table if exists ods.yw_share_require_relation;
create external table ods.yw_share_require_relation(
id                                                     int             comment '主键id',
share_id                                               int             comment '分享id',
share_type                                             int             comment '分享类型:1单楼盘2多楼盘3文章',
employee_id                                            int             comment '咨询师id',
order_id                                               bigint          comment '订单id',
total_price_min                                        int             comment '最小总价',
total_price_max                                        int             comment '最大总价',
first_price_min                                        int             comment '最小首付',
first_price_max                                        int             comment '最大首付',
acreage_min                                            int             comment '最小面积',
acreage_max                                            int             comment '最大面积',
project_type                                           string          comment '业态类型,1住宅2别墅3商住4其他5未知',
qualifications                                         int             comment '是否有资质,1有2无3正在办理',
house_type                                             string          comment '户型，逗号分隔，1一居2二居3三居4四居5五居及以上6loft',
purchase_purpose                                       int             comment '购房目的:1刚需，2改善，3投资，4自助+投资',
option_status                                          string          comment '以上字段是否要展示，json储存，0展示1不展示',
creator                                                int             comment '创建者',
updator                                                int             comment '修改者',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_share_require_relation'
;
