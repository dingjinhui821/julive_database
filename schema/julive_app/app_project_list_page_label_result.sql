drop table if exists julive_app.app_project_list_page_label_result;
create table julive_app.app_project_list_page_label_result(
select_city    string  comment '用户搜索城市',
label          string  comment '标签',
label_uv       string  comment '点击某一标签总人数',
total_uv       string  comment '该城市总人数',
user_scale     string  comment '点击标签占比',
user_sort      string  comment '点击某一标签总人数排序',
etl_time       string  comment '跑数时间'
)
stored as parquet 
;