drop table if exists ods.cj_header;
create external table ods.cj_header(
employee_id                                            bigint          comment '员工id',
create_datetime                                        int             comment '',
creator                                                bigint          comment '',
update_datetime                                        int             comment '',
updator                                                bigint          comment '',
employee_name                                          string          comment '员工名字',
sex                                                    int             comment '性别:1男,2女',
avatar                                                 string          comment '头像',
personal_img                                           string          comment '诙谐头像',
introduce                                              string          comment '介绍',
status                                                 int             comment '审核状态:1 未提交2 待审核3 审核失败4 审核成功',
mobile                                                 string          comment '手机号',
wechat_img                                             string          comment '微信头像',
success_num                                            int             comment '成交量',
see_num                                                int             comment '带看量',
click_num                                              int             comment '点击量',
high_rate                                              double          comment '好评率',
tag                                                    string          comment '用竖线|隔开',
refuse_content                                         string          comment '拒绝原因',
order_num                                              int             comment '上户数',
city_id                                                int             comment '城市id，城市化专用的分隔标记',
good_skill                                             int             comment '擅长技能（1:楼盘行家 2:踩盘活地图 3:市场分析行家 4:楼盘勘探师 5:政策解读达人 6:行业探盘达人 7:行业分析能手 8:费用计算能手 9:带看达人 10:其他「此选项擅长-补充说明必填」）',
skill_des                                              string          comment '擅长-补充说明',
flat_sit_img                                           string          comment '平坐式照片',
high_sit_img                                           string          comment '高坐式照片',
video_src                                              string          comment '咨询师视频地址 存储格式 例:{mp4:/video/mp4/employee_zhangbo.mp4,ogv:/video/ogv/employee_zhangbo.ogv,webm:/video/web/employee_zhangbo.webm}',
academy                                                string          comment '毕业院校',
education                                              string          comment '学历',
square_sit_img                                         string          comment '方正式照片',
school_attributes                                      int             comment '毕业学校类型',
etl_time                                               string          comment 'ETL跑数时间'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/ods/cj_header'
;
