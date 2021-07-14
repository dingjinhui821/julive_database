drop table if exists ods.ex_project_learn;
create external table ods.ex_project_learn(
id                                                     int             comment '',
ex_order_id                                            int             comment 'bd单id',
project_id                                             int             comment '楼盘id',
require_complete_datetime                              int             comment '系统规定完成学习时间',
status                                                 int             comment '是否完成 1完成 2未完成',
progress                                               double          comment '学习进度',
product_type                                           string          comment '产品类型(逗号分隔) 1住宅，2别墅，3写字楼，4商办，5商住 逗号分隔',
size_type                                              string          comment '体量大小(逗号分隔) 1表示 0-100套，2表示100-500套，3表示 500-1000套，4表示1000套以上   逗号分隔',
main_house_type                                        string          comment '主力户型(逗号分隔) 1 一居，2 二居，3 三居，4 四居，5 五居及以上',
min_price                                              double          comment '最小单价',
max_price                                              double          comment '最大单价',
min_total_price                                        double          comment '最小总价',
max_total_price                                        double          comment '最大总价',
sale_status                                            int             comment '销售状态 在售，未售，待售，售罄，排卡中',
is_open                                                int             comment '开盘情况 1已开盘 2未开盘',
sale_task                                              string          comment '销售任务',
sale_num                                               int             comment '销售量',
customer_situation                                     int             comment '客户量情况',
agency_company                                         string          comment '代理公司',
channel_company                                        string          comment '渠道公司',
media_ecom                                             string          comment '媒体电商',
office_open                                            int             comment '售楼处是否开放 1是 2否',
advantage                                              string          comment '项目优势',
disadvantage                                           string          comment '项目劣势',
ex_order_status                                        int             comment 'bd单状态',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
is_delete                                              int             comment '是否删除 1是 2否',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/ex_project_learn'
;
