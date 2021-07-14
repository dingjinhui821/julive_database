
drop table if exists julive_topic.topic_lable_user_lifecycle_app_rating;
create table julive_topic.topic_lable_user_lifecycle_app_rating(
global_id                                     string comment '用户ID',
comjia_unique_id                              string comment '根据用户设备生成的标识',
julive_id                                     string comment '居理ID',
user_name                                     string comment '用户名称',
user_mobile                                   string comment '用户手机号',
select_city                                   string comment '用户搜索城市',
emp_id                                        string comment '员工id',
emp_name                                      string comment '员工姓名', 
manufacturer                                  string COMMENT '手机品类', 
app_version                                   string COMMENT 'app版本号', 
os_system                                     string COMMENT '手机系统', 
os_version                                    string COMMENT '手机系统版本', 
is_close                                      string comment '是否关单1-关，0-未关',
latest_distribute_date                        string comment '关单时间',
comjia_unique_id_valid_status                 INT    COMMENT 'APP的comjia_unique_id的有效状态，1有效2无效，更新自打开app和push通道反馈',
comjia_unique_id_valid_status_update_datetime BIGINT COMMENT 'comjia_unique_id_valid_status的更新时间',
first_visit_daytime                           string comment '首次访问时间',
final_visit_daytime                           string comment '最后一次访问时间',
is_7day_live                                  string comment '7日内是否活跃', 
is_30day_live                                 string comment '30日内是否活跃',
create_date_min                               string comment '`线索创建日期-min',           
create_date_max                               string comment '`线索创建日期-max',          
distribute_date_min                           string comment '`上户日期-min,',                            
distribute_date_max                           string comment '`上户日期-max,',                             
first_see_date_min                            string comment '`首次带看日期-min',              
first_see_date_max                            string comment '`首次带看日期-max',              
first_subscribe_date_min                      string comment '`首次认购日期-min',       
first_subscribe_date_max                      string comment '`首次认购日期-max',       
first_sign_date_min                           string comment '`首次签约日期-min',               
first_sign_date_max                           string comment '`首次签约日期-max',                    
--浏览模块         
visit_day_cnt_rating                          string comment '总计来访天数得分', 
visit_cnt_per_day_rating                      string comment '日均来访次数得分', 
visit_duration_per_day_rating                 string comment '日均来访时长得分', 
pc_day_avg_rating                             string comment '日均点击楼盘卡片次数得分', 
htc_day_avg_rating                            string comment '日均点击户型卡片次数得分', 
         
visit_hour_rating                             string comment '来访hour(0-23)得分', 
call_video_cnt_rating                         string comment 'C端版块:视频看房次数得分', 
call_video_duration_rating                    string comment 'C端版块:视频看房时长得分',
cp_cont_days_rating                           string comment '连续点击楼盘卡片天数得分',
         
htc_cont_days_rating                          string comment '连续点击户型卡片天数得分', 
         
se_cont_days_rating                           string comment '连续搜索天数得分', 
fp_cont_pv_rating                             string comment '连续楼盘过滤总次数得分', 
cp_cont_pv_rating                             string comment '连续点击楼盘卡片总次数得分',
htc_cont_pv_rating                            string comment '连续点击户型卡片总次数得分', 
se_cont_pv_rating                             string comment '连续搜索总次数得分', 
fp_cont_days_rating                           string comment '连续楼盘过滤天数得分',
          
         
         
--c端行为         
activity_cnt_rating                           string comment 'C端版块: 参加活动次数得分', 
qa_ask_cnt_rating                             string comment 'C端版块: 问问提问数得分', 
qa_click_cnt_rating                           string comment 'C端版块: 问问点击数得分', 
qa_view_duration_rating                       string comment 'C端版块: 问问浏览数得分', 
info_article_cnt_rating                       string comment 'C端版块: 情报局文章点击数得分', 
info_video_cnt_rating                         string comment 'C端版块: 情报局视频点击数得分', 
info_project_cnt_rating                       string comment 'C端版块: 情报局楼盘点击数得分', 
info_question_cnt_rating                      string comment 'C端版块: 情报局问答点击数得分', 
leave_phone_cnt_rating                        string comment 'C端版块: 留电次数得分', 
service_chat_cnt_rating                       string comment 'C端版块: 在线咨询次数得分', 
service_chat_duration_rating                  string comment 'C端版块: 在线咨询时长得分', 
booking_cnt_rating                            string comment 'C端版块: 在线订房数得分', 
submit_question_cnt_rating                    string comment 'C端: 问题提交次数得分', 
project_comment_cnt_rating                    string comment 'C端: 楼盘平均数得分', 
send_comment_cnt_rating                       string comment 'C端: 一般评论数得分', 
share_cnt_rating                              string comment 'C端: 楼盘share次数得分', 
like_cnt_rating                               string comment 'C端: 楼盘like数得分', 
love_cnt_rating                               string comment 'C端: 楼盘love数得分', 
follow_cnt_rating                             string comment 'C端: 楼盘follow数得分', 
collect_cnt_rating                            string comment 'C端: 楼盘collect数得分', 
         
--app          
real_estate_cnt_rating                        string comment '手机房产类App数量得分', 
house_decoration_cnt_rating                   string comment '手机装修类App数量得分', 
car_cnt_rating                                string comment '手机汽车类App数量得分', 
marriage_cnt_rating                           string comment '手机婚恋类App数量得分', 
mombaby_cnt_rating                            string comment '手机孕婴类App数量得分', 
investment_cnt_rating                         string comment '手机金融理财类App数量得分',
total_points                                  string comment '用户总得分'
)
STORED as parquet;

