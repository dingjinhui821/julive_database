drop table if exists julive_fact.fact_employee_period_indi;
create external table julive_fact.fact_employee_period_indi
(
employee_id         int    COMMENT '员工id', 
city                string COMMENT '城市名称', 
school_attributes   string COMMENT '学校性质', 
entry_date          string COMMENT '入职时间', 
graduation_date     string COMMENT '毕业时间', 
batch               string COMMENT '批次', 
fresh_graduate      int    COMMENT '是否应届，1为应届，2为非应届', 
leave_date          string COMMENT '离职日期', 
full_date           string COMMENT '转正日期', 
entry_leave_cycle   int    COMMENT '入离职周期', 
full_cycle          int    COMMENT '转正周期', 
is_full             int    COMMENT '是否转正，1为是，0为否', 
full_on_time        int    COMMENT '是否按时转正，1为是，0为否', 
30_day_leave        int    COMMENT '30天内是否离职', 
60_day_leave        int    COMMENT '60天内是否离职', 
90_day_leave        int    COMMENT '90天内是否离职', 
180_day_leave       int    COMMENT '180天内是否离职', 
is_table2           int    COMMENT '是否破蛋', 
broken_date         string COMMENT '破蛋时间', 
broken_cycle        int    COMMENT '破蛋周期'
) COMMENT '员工周期表' 
STORED AS textfile;
