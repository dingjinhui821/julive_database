drop table if exists julive_dim.dim_refrush_activate_balcklist_info;
create table julive_dim.dim_refrush_activate_balcklist_info(
p_ip             string comment '刷量ip',
p_model          string comment '刷量机型',
comjia_unique_id string comment '刷量激活用户'
)
comment'刷量激活黑名单'
partitioned by(pdate string)
stored as parquet
;
