#!/bin/bash
#!/bin/bash
#hive -e "show tables from julive_app; " > db.txt

for line in `cat db.txt`
do
 echo $line
 hive -e "show create table julive_app.$line;" >>$line.sql
done
