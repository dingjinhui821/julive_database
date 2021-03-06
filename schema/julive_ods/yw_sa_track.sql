drop table if exists julive_bak.yw_sa_track;
create external table julive_bak.yw_sa_track(
id                                                     bigint          comment '',
event                                                  string          comment '',
date                                                   string          comment '',
time                                                   string          comment '',
district_id                                            string          comment '',
frompage                                               string          comment '',
frommodule                                             string          comment '',
fromitem                                               string          comment '',
fromitemindex                                          string          comment '',
topage                                                 string          comment '',
tomodule                                               string          comment '',
search_content                                         string          comment '',
tab_id                                                 string          comment '',
project_id                                             string          comment '',
support_type                                           string          comment '',
plate                                                  string          comment '',
picture_type_id                                        string          comment '',
house_type                                             string          comment '',
subway                                                 string          comment '',
`loop`                                                 string          comment '',
total_price_min                                        string          comment '',
total_price_max                                        string          comment '',
average_price_min                                      string          comment '',
average_price_max                                      string          comment '',
project_type                                           string          comment '',
features                                               string          comment '',
sale_status                                            string          comment '',
frompage_id                                            string          comment '',
login_status                                           string          comment '',
feedback_content                                       string          comment '',
district                                               string          comment '',
employee_id                                            string          comment '',
banner_id                                              string          comment '',
order_id                                               string          comment '',
tag_name                                               string          comment '',
house_type_id                                          string          comment '',
source_page                                            string          comment '',
contrastive_number                                     string          comment '',
button_type                                            string          comment '',
first_stage_id                                         string          comment '',
second_stage_id                                        string          comment '',
article_title_id                                       string          comment '',
coin_name                                              string          comment '',
coordinate_id                                          string          comment '',
e_product_id                                           string          comment '',
u_product_id                                           string          comment '',
screen_width                                           string          comment '',
model                                                  string          comment '',
user_id                                                string          comment '',
distinct_id                                            string          comment '',
app_version                                            string          comment '',
city                                                   string          comment '',
event_duration                                         string          comment '',
browser                                                string          comment '',
login_employee_id                                      string          comment '',
employee_leader_id                                     string          comment '',
area_id                                                string          comment '',
sector_id                                              string          comment '',
index_time                                             string          comment '',
longitude                                              string          comment '',
latitude                                               string          comment '',
login_city_id                                          string          comment '',
action                                                 string          comment '',
article_id                                             string          comment '',
bannerid                                               string          comment '',
building_id                                            string          comment '',
city_id                                                string          comment '',
code                                                   string          comment '',
`index`                                                string          comment '',
landmark_id                                            string          comment '',
landmark_type                                          string          comment '',
module_name                                            string          comment '',
navigation                                             string          comment '',
type                                                   string          comment '',
view_time                                              string          comment '',
create_datetime                                        bigint          comment '????????????',
update_datetime                                        bigint          comment '????????????',
etl_time                                               string          comment 'ETL????????????'
) row format delimited fields terminated by '\001' 
lines terminated by '\n' 
stored as textfile  
location '/dw/julive_bak/yw_sa_track'
;
