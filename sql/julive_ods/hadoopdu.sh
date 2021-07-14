#!/bin/bash
##################
#@author: chichuduxing
#@date: 20161011
##################

##加载Hadoop环境变量
##略过

function showhelp()
{
	echo "###################################!!! you must input at least two params !!!###################################"
	echo "#Usage: [sh `basename $0` origin|-o hdfspath] show info with filename's ascending order ,eg: sh `basename $0` origin /home"
	echo "#Usage: [sh `basename $0` asc|-a hdfspath] show info with size's ascending order,eg: sh `basename $0` asc /home"
	echo "#Usage: [sh `basename $0` desc|-d hdfspath] show info with size's descending order,eg: sh `basename $0` desc /home"
	echo "###############################################################################################################"
}

#首先判断输入参数的个数是否符合要求
if [ $# -ne 2 ]; then
	#如果传递进来的参数个数不等于2，则记录错误并退出
	showhelp
	#退出
	exit 1
fi

function origin()
{
	hadoop fs -du $1 | awk '{if(1==NR){print $0}else{size=$1/1024;if(size<1024){printf("%10.3f KB\t%s\n",size,$2);}else{size=size/1024;if(size<1024){printf("\033[36m%10.3f MB\t%s\n\033[0m",size,$2);}else{size=size/1024;if(size<1024){printf("\033[35m%10.3f GB\t%s\n\033[0m",size,$2);}else{size=size/1024;printf("\033[31m%10.3f TB\t%s\n\033[0m",size,$2);}}}}}'
}

function order()
{
	hadoop fs -du $1 | tail -n +2 | sort $2 -n | awk '{size=$1/1024;if(size<1024){printf("%10.3f KB\t%s\n",size,$2);}else{size=size/1024;if(size<1024){printf("\033[36m%10.3f MB\t%s\n\033[0m",size,$2);}else{size=size/1024;if(size<1024){printf("\033[35m%10.3f GB\t%s\n\033[0m",size,$2);}else{size=size/1024;printf("\033[31m%10.3f TB\t%s\n\033[0m",size,$2);}}}}'
}

function asc()
{
	order $1
}

function desc()
{
	order $1 -r
}

case "$1" in
		origin|-o)
				origin $2
				;;
		asc|-a)
				asc $2
				;;
		desc|-d)
				desc $2
				;;
		*)
				showhelp
				exit 1
esac
exit 0
