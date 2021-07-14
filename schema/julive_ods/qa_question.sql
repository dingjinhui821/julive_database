drop table if exists ods.qa_question;
create external table ods.qa_question(
id                                                     bigint          comment '',
creator                                                bigint          comment '创建人',
create_datetime                                        int             comment '创建时间',
updator                                                bigint          comment '更新人',
update_datetime                                        int             comment '更新时间',
city_id                                                int             comment '城市id',
order_id                                               bigint          comment '订单id',
questioner_type                                        int             comment '提问人类型，1用户，2咨询师',
questioner_id                                          bigint          comment '提问人id',
questioner_nickname                                    string          comment '提问人昵称',
type                                                   string          comment '问题类型id，逗号分隔',
title                                                  string          comment '问题标题',
status                                                 int             comment '状态，1正常，2删除',
follow_type                                            int             comment '跟进类型，1联系，2带看',
follow_id                                              bigint          comment '跟进id',
total_price_min                                        int             comment '总价预算，最小总价',
total_price_max                                        int             comment '总价预算，最大总价',
district_id                                            string          comment '关注区域id，逗号分隔',
purchase_purpose                                       int             comment '购房目的:1刚需，2改善，3投资，4自助+投资',
personal                                               int             comment '用户本人可见',
frontend                                               int             comment '网站展示',
backend                                                int             comment '咨询师内部',
click_num                                              int             comment '问题点击量',
is_import_headline                                     int             comment '是否导入楼市头条（1:是,0:否）',
share_num                                              int             comment '问问分享次数',
browse_num                                             int             comment '问问浏览次数',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/qa_question'
;
