drop table if exists ods.yw_article_attr;
create external table ods.yw_article_attr(
id                                                     int             comment '主键id',
article_absorbed_id                                    int             comment '文章表id',
cj_project_headline_id                                 int             comment 'c端数据-主键id',
view_num                                               int             comment 'c端数据-实际浏览量',
category_id                                            int             comment '分类id',
`group`                                                int             comment '头条分组:1.焦点、2.推房、3.百科、4.品牌、5.个人',
is_top                                                 int             comment '是否置顶 1，否 2，置顶',
is_new                                                 int             comment '是否显示new 1，否 2，是',
browse_count                                           int             comment '浏览次数',
forward_count                                          int             comment '转发次数',
sort_val                                               double          comment '排序值',
article_update_datetime                                int             comment '文章创建时间',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建者',
updator                                                int             comment '更新者',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_article_attr'
;
