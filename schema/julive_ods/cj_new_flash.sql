drop table if exists ods.cj_new_flash;
create external table ods.cj_new_flash(
id                                                     int             comment '主键id',
table_belong                                           int             comment '所属表  会在cjnewflashconstanst中写常量映射',
object_id                                              int             comment '这条数据对应表的主键id',
fraction                                               double          comment '数据得分 用作排序',
status                                                 int             comment '是否展示 1 是 2 否',
city_id                                                int             comment '城市id',
is_top                                                 int             comment '是否置顶 1 是 2 否',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '修改时间',
tag                                                    string          comment '标签，对应每个表的数据类型（分隔符:,）',
tag_sum                                                int             comment '标签之和',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_new_flash'
;
