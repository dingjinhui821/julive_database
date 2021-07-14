CREATE VIEW `julive_fact.fact_consultant_call_log_dtl` AS SELECT `called_id` AS `called_id`, `recorder_id` AS `recorder_id`, `clue_id` AS `clue_id`, `distribute_date` AS `distribute_date`, `project_type` AS `project_type`, `project_type_name` AS `project_type_name`, `interest_project` AS `interest_project`, `intent_low_date` AS `intent_low_date`, `clue_city_id` AS `clue_city_id`, `clue_city_name` AS `clue_city_name`, `clue_city_seq` AS `clue_city_seq`, `customer_intent_city_id` AS `customer_intent_city_id`, `customer_intent_city_name` AS `customer_intent_city_name`, `customer_intent_city_seq` AS `customer_intent_city_seq`, `channel_id` AS `channel_id`, `channel_name` AS `channel_name`, `media_id` AS `media_id`, `media_name` AS `media_name`, `module_id` AS `module_id`, `module_name` AS `module_name`, `device_id` AS `device_id`, `device_name` AS `device_name`, `emp_id` AS `emp_id`, `emp_name` AS `emp_name`, `direct_leader_id` AS `direct_leader_id`, `direct_leader_name` AS `direct_leader_name`, `indirect_leader_id` AS `indirect_leader_id`, `indirect_leader_name` AS `indirect_leader_name`, `dept_id` AS `dept_id`, `dept_name` AS `dept_name`, `entry_date` AS `entry_date`, `full_date` AS `full_date`, `offjob_date` AS `offjob_date`, `full_type` AS `full_type`, `full_type_tc` AS `full_type_tc`, `caller_type` AS `caller_type`, `alerting_time` AS `alerting_time`, `connect_time` AS `connect_time`, `release_time` AS `release_time`, `call_result` AS `call_result`, `caller_area` AS `caller_area`, `called_area` AS `called_area`, `is_valid` AS `is_valid`, `is_first_called` AS `is_first_called`, `call_duration` AS `call_duration`, `bill_duration` AS `bill_duration`, `call_cost` AS `call_cost`, `create_time` AS `create_time`, `etl_time` AS `etl_time` FROM (select 

`t1`.`id`                         as `called_id`,
`t1`.`recorder_id`                as `recorder_id`,

-- ????/???? 
`t1`.`order_id`                   as `clue_id`,
`t2`.`distribute_date`            as `distribute_date`,
`t2`.`project_type`               as `project_type`,
`t2`.`project_type_name`          as `project_type_name`,
`t2`.`interest_project`           as `interest_project`,
to_date(`t2`.`intent_low_time`)   as `intent_low_date`,

`t2`.`city_id`                    as `clue_city_id`,
`t2`.`city_name`                  as `clue_city_name`,
`t2`.`city_seq`                   as `clue_city_seq`,

`t2`.`customer_intent_city_id`    as `customer_intent_city_id`,-- 20191015 
`t2`.`customer_intent_city_name`  as `customer_intent_city_name`,-- 20191015 
`t2`.`customer_intent_city_seq`   as `customer_intent_city_seq`,-- 20191015 

`t2`.`channel_id`                 as `channel_id`,
`t3`.`channel_name`               as `channel_name`,
`t3`.`media_id`                   as `media_id`,
`t3`.`media_name`                 as `media_name`,
`t3`.`module_id`                  as `module_id`,
`t3`.`module_name`                as `module_name`,
`t3`.`device_id`                  as `device_id`,
`t3`.`device_name`                as `device_name`,

-- ??????? 
`t1`.`employee_id`                                       as `emp_id`,
`t4`.`emp_name`                                          as `emp_name`,
coalesce(`t4`.`direct_leader_id`,`t1`.`employee_manager_id`) as `direct_leader_id`,   -- ??ID
`t4`.`direct_leader_name`                                as `direct_leader_name`, -- ???? 
`t4`.`indirect_leader_id`                                as `indirect_leader_id`,
`t4`.`indirect_leader_name`                              as `indirect_leader_name`,
-- 
-- case 
-- when t4.dept_level = 8 then t4.dept_leader_id_seventh 
-- when t4.dept_level = 7 then t4.dept_leader_id_sixth 
-- when t4.dept_level = 6 then t4.dept_leader_id_fifth 
-- when t4.dept_level = 5 then t4.dept_leader_id_fourth 
-- when t4.dept_level = 4 then t4.dept_leader_id_third 
-- when t4.dept_level = 3 then t4.dept_leader_id_second 
-- else t4.dept_leader_id_first end as indirect_leader_id,
-- 
-- case 
-- when t4.dept_level = 8 then t4.dept_leader_name_seventh 
-- when t4.dept_level = 7 then t4.dept_leader_name_sixth 
-- when t4.dept_level = 6 then t4.dept_leader_name_fifth 
-- when t4.dept_level = 5 then t4.dept_leader_name_fourth 
-- when t4.dept_level = 4 then t4.dept_leader_name_third 
-- when t4.dept_level = 3 then t4.dept_leader_name_second 
-- else t4.dept_leader_id_first end as indirect_leader_name,
-- 
`t1`.`department_id`       as `dept_id`,
`t4`.`dept_name`           as `dept_name`,
`t4`.`entry_date`          as `entry_date`,
`t4`.`full_date`           as `full_date`,
`t4`.`offjob_date`         as `offjob_date`,
`t4`.`full_type`           as `full_type`,
`t4`.`full_type_tc`        as `full_type_tc`,

`t1`.`caller_type`,
`t1`.`alerting_time`,
`t1`.`connect_time`,
`t1`.`release_time`,
`t1`.`call_result`,
`t1`.`caller_area`,
`t1`.`called_area`,

if(`t1`.`call_result` = "ANSWERED",1,0)                                               as `is_valid`,
if(row_number()over(partition by `t1`.`order_id` order by `t1`.`release_time` asc)=1,1,0) as `is_first_called`,
`t1`.`call_duration`,
`t1`.`bill_duration`,
`t1`.`call_cost`,
from_unixtime(`t1`.`create_datetime`) as `create_time`,
`t1`.`etl_time` 

from `ods`.`yw_sys_number_talking` `t1` 
left join `julive_dim`.`dim_clue_info` `t2` on `t1`.`order_id` = `t2`.`clue_id` 
left join `julive_dim`.`dim_channel_info` `t3` on `t2`.`channel_id` = `t3`.`channel_id` 
left join `julive_dim`.`dim_ps_employee_info` `t4` on `t1`.`employee_id` = `t4`.`emp_id` and from_unixtime(`t1`.`create_datetime`,'yyyyMMdd') = `t4`.`pdate` 

where `t1`.`call_result` = "ANSWERED" 
and `t1`.`order_id` != 0 
and `t1`.`order_id` is not null 
and `t1`.`call_duration` <= 7200 
and from_unixtime(`t1`.`create_datetime`,'yyyy-MM-dd') >= date_add(current_date(),-90)) `julive_fact.fact_consultant_call_log_dtl`
