drop table if exists ods.cj_project_ext;
create external table ods.cj_project_ext(
project_id                                             int             comment '楼盘id',
industry                                               string          comment '产业',
school                                                 string          comment '学校',
trade                                                  string          comment '商业',
medical                                                string          comment '医疗',
environment                                            string          comment '环境',
traffic                                                string          comment '交通',
project_address                                        string          comment '楼盘位置',
sale_office_address                                    string          comment '售楼处位置',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
design_company                                         string          comment '设计单位',
contruct_company                                       string          comment '承建单位',
relation_take_developers                               string          comment '拿地开发商id，逗号分隔',
relation_trader_developers                             string          comment '操盘开发商id，逗号分隔',
relation_proxy_company                                 string          comment '代理公司id，逗号分隔',
relation_channel_company                               string          comment '渠道公司id，逗号分隔',
relation_tag_ids                                       string          comment '标签id，逗号分隔',
relation_seo_tag_ids                                   string          comment 'seo标签id，逗号分隔',
project_note                                           string          comment '楼盘备注',
latest_show_time                                       int             comment '最新显示时间',
project_comment                                        string          comment '咨询师对楼盘的点评',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_ext'
;
