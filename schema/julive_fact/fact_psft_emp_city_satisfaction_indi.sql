drop table if exists julive_fact.fact_psft_emp_city_satisfaction_indi;
CREATE EXTERNAL TABLE  julive_fact.fact_psft_emp_city_satisfaction_indi (
   emp_id  bigint COMMENT '咨询师ID', 
   emp_name  string COMMENT '咨询师名称', 
   region    string COMMENT '所属大区',
   adjust_city_id  int COMMENT '员工核算城市ID', 
   adjust_city_name  string COMMENT '员工核算城市城市', 
   adjust_city_seq  string COMMENT '带开城顺序的员工核算城市城市', 
   direct_leader_id  int COMMENT '咨询师直接领导ID', 
   direct_leader_name  string COMMENT '咨询师直接领导名称', 
   indirect_leader_id  int COMMENT '咨询师经理ID', 
   indirect_leader_name  string COMMENT '咨询师经理名称', 
   dept_id  bigint COMMENT '部门ID', 
   dept_name  string COMMENT '部门名称', 
   post_id  bigint COMMENT '职位ID', 
   post_name  string COMMENT '职位名称', 
   adjust_distribute_num  int COMMENT '核算上户量', 
   first_distribute_num  int COMMENT '首次分配上户量', 
   first_called_report_num  int COMMENT '发送首电报告数量', 
   first_called_report_viewed_num  int COMMENT '首电报告被浏览数量', 
   see_report_num  int COMMENT '发送带看报告数量', 
   see_report_viewed_num  int COMMENT '带看报告被浏览数量', 
   first_called_reword_num  int COMMENT '首电报告被打赏数量', 
   see_reword_num  int COMMENT '带看报告被打赏数量', 
   see_num  int COMMENT '带看量', 
   sign_contains_cancel_ext_num  int COMMENT '签约量-含退含外联', 
   new_cust_follow_low_num  int COMMENT '新上户跟进频率低数量', 
   day_follow_low_num  int COMMENT '日常维护频率太低数量', 
   clue_close_nonsee_num  int COMMENT '未带看关闭数量', 
   rv_final_grade_0_num  int COMMENT '未带看关闭-0分评价量', 
   rv_final_grade_1_num  int COMMENT '未带看关闭-1分评价量', 
   rv_final_grade_2_num  int COMMENT '未带看关闭-2分评价量', 
   rv_final_grade_3_num  int COMMENT '未带看关闭-3分评价量', 
   rv_final_grade_4_num  int COMMENT '未带看关闭-4分评价量', 
   rv_final_grade_5_num  int COMMENT '未带看关闭-5分评价量', 
   rv_hastxt_comment_num  int COMMENT '未带看关闭-文字评价量', 
   rv_txt_comment_lt15_num  int COMMENT '未带看关闭-<15字评价量', 
   rv_txt_comment_ge15_num  int COMMENT '未带看关闭->=15字评价量', 
   continue_server_0_num  int COMMENT '未带看关闭-愿意接受服务未知数量', 
   continue_server_1_num  int COMMENT '未带看关闭-愿意接受服务数量', 
   continue_server_2_num  int COMMENT '未带看关闭-不愿意接受服务数量', 
   txt_comment_lt15_cs0_num  int COMMENT '未带看关闭-<15字文评且愿意接受服务数量', 
   txt_comment_lt15_cs1_num  int COMMENT '未带看关闭-<15字文评且不愿意接受服务数量', 
   txt_comment_lt15_cs2_num  int COMMENT '未带看关闭-<15字文评且愿意接受服务未知数量', 
   txt_comment_ge15_cs0_num  int COMMENT '未带看关闭->=15字文评且愿意接受服务数量', 
   txt_comment_ge15_cs1_num  int COMMENT '未带看关闭->=15字文评且不愿意接受服务数量', 
   txt_comment_ge15_cs2_num  int COMMENT '未带看关闭->=15字文评且愿意接受服务未知数量', 
   sc_final_grade_0_num  int COMMENT '带看客户评价-0分评价量', 
   sc_final_grade_1_num  int COMMENT '带看客户评价-1分评价量', 
   sc_final_grade_2_num  int COMMENT '带看客户评价-2分评价量', 
   sc_final_grade_3_num  int COMMENT '带看客户评价-3分评价量', 
   sc_final_grade_4_num  int COMMENT '带看客户评价-4分评价量', 
   sc_final_grade_5_num  int COMMENT '带看客户评价-5分评价量', 
   sc_hastxt_comment_num  int COMMENT '带看客户评价-文字评价量', 
   sc_txt_comment_lt30_num  int COMMENT '带看客户评价-<30字评价量', 
   sc_txt_comment_ge30_num  int COMMENT '带看客户评价->=30字评价量', 
   nps_lt90_num  int COMMENT '带看客户评价->nps<90评价量', 
   nps_ge90_num  int COMMENT '带看客户评价->nps>=90评价量', 
   txt_comment_lt30_nps_lt90_num  int COMMENT '带看客户评价-<30字且NPS<90评价量', 
   txt_comment_lt30_nps_gt90_num  int COMMENT '带看客户评价-<30字且NPS>=90评价量', 
   txt_comment_ge30_nps_lt90_num  int COMMENT '带看客户评价->=30字且NPS< 90评价量', 
   txt_comment_ge30_nps_gt90_num  int COMMENT '带看客户评价->=30字且NPS>=90评价量', 
   sign_comment_num  int COMMENT '签约评价量', 
   sign_comment_good_num  int COMMENT '签约评价加精量', 
   sign_comment_nongood_num  int COMMENT '签约评价未加精量', 
   cust_recommend_num  int COMMENT '友介客户数量', 
   cust_comp_num  int COMMENT '投诉量', 
   first_called_report_shared_num  int COMMENT '首电报告被转发(分享)数量', 
   see_reword_shared_num  int COMMENT '带看报告被转发（分享）数量', 
   etl_time  string COMMENT 'ETL跑数时间')
COMMENT '员工-城市满意度指标周期事实表'
PARTITIONED BY ( 
   pdate  string COMMENT '数据日期:yyyyMMdd')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://optimuspro01:8020/dw/julive_fact/fact_psft_emp_city_satisfaction_indi'
TBLPROPERTIES (
  'transient_lastDdlTime'='1572352737')