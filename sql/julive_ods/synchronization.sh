 echo table_name=$1
 v=$(mysql -uroot -pQemENw\>O0.Z -h192.168.10.11 julive_dw -e "SELECT IFNULL((select 'Y' from julive_dw.etl_collect_db_file where file_id=$table_namm
 e),'N')")
 #查询julive_dw.etl_collect_db_file表中要同步的表是否存在
 
 echo $v
 c=$v
