#!/bin/bash
#!/bin/bash
#hive -e "show tables from julive_fact; " > db.txt

for line in `cat db.txt`
do
 echo $line
 hive -e "show create table julive_fact.$line;" >>$line.sql
done
