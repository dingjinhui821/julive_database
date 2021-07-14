drop table if exists ods.cj_project_evaluation;
create external table ods.cj_project_evaluation(
id                                                     int             comment '主键id',
city_id                                                int             comment '城市id',
project_id                                             int             comment '楼盘id',
status                                                 int             comment '状态 1 启用，2 禁用 默认 0',
key_words                                              string          comment '关键词，|分隔存入',
advantage                                              string          comment '项目优势',
disadvantage                                           string          comment '项目劣势',
user_match                                             string          comment '客群匹配',
common_tips                                            int             comment '小居有话说:项目综述',
summary                                                string          comment '生活配套:项目概述，json结构',
around_info                                            string          comment '生活配套:周边配套及等级，json结构',
around_highlight                                       string          comment '生活配套:周边配套最大亮点',
around_tips                                            int             comment '小居有话说:周边配套',
surrounding                                            string          comment '居住环境:小区外部环境，json',
surrounding_img                                        string          comment '小区外部环境图片，json',
developer                                              int             comment '项目开发商，存开发id',
facility                                               string          comment '小区硬件设施',
facility_img                                           string          comment '小区硬件设施图片，json',
recommend_house                                        string          comment '推荐户型，json',
delivery_standard                                      string          comment '交付标准',
surrounding_tips                                       int             comment '小居有话说:居住环境',
policy                                                 string          comment '区域潜力:政策规划 json',
population                                             string          comment '区域潜力:人口产业 json',
land                                                   string          comment '区域潜力:土地价值 json',
land_img                                               string          comment '区域潜力:土地价值资料图片 json',
summarize                                              string          comment '总结语',
create_datetime                                        int             comment '创建时间',
update_datetime                                        int             comment '更新时间',
creator                                                int             comment '创建人',
updator                                                int             comment '更新人',
life_score                                             int             comment '生活配套得分',
live_score                                             int             comment '居住环境得分',
region_score                                           int             comment '区域潜力得分',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_project_evaluation'
;
