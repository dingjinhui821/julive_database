drop table if exists julive_app.app_project_list_page_label_click;
create table julive_app.app_project_list_page_label_click(
select_city    string  comment '用户搜索城市',
global_id      string  comment '用户唯一标示',
label          string  comment '标签',
etl_time       string  comment '跑数时间'
)
partitioned by (pdate string comment '数据日期')
stored as parquet 
;
