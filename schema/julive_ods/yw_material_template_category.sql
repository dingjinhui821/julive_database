drop table if exists ods.yw_material_template_category;
create external table ods.yw_material_template_category(
id                                                     int             comment '',
template_id                                            int             comment '模板表id',
type                                                   int             comment '模块类型:10,开头语 20,当前购房需求 30:当前购房情况 40:市场情况解读 50:关注区域分析 60:匹配楼盘推荐 70:其他问题解答 80:结束语',
sort                                                   int             comment '排序',
title                                                  string          comment '衔接语',
status                                                 int             comment '状态（删除=-1 未删除=1)',
project_id                                             int             comment '楼盘id',
content                                                string          comment '内容',
creator                                                int             comment '创建人',
updator                                                int             comment '状态（删除=-1 未删除1）',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
delete_datetime                                        int             comment '删除时间',
module_name                                            string          comment '模块名称',
relation_id                                            int             comment '关联信息id',
name                                                   string          comment '名称',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/yw_material_template_category'
;
