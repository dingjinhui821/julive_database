drop table if exists julive_app.app_push_hottest_house;
create table julive_app.app_push_hottest_house(
city_id          string comment '楼盘地理城市',
project_id       string comment '楼盘id',
house_id         string comment '户型id',
room_type        string comment '厅室类别:0不限, 1一居, 2二居, 3三居, 4四居, 5五居及以上, 6loft',
good_desc        string comment '优势描述',
house_tag        string comment '户型标签',
master_bed_room  string comment '主卧居室详解',
toilet           string comment '卫生间居室详解',
living_room      string comment '客厅详解',
status           int    COMMENT '状态，1: 未售  2:在售  3:售罄 4:待售',
house_uv         string comment '户型近8个月天累计uv',
hot_sort         int    comment '户型热度排名'
)
stored as parquet;

