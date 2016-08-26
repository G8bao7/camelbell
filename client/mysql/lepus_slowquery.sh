#!/bin/bash
#****************************************************************#
# ScriptName: /usr/local/sbin/lepus_slowquery.sh
# Create Date: 2014-03-25 10:01
# Modify Date: 2014-03-25 10:01
#***************************************************************#


# get local ip
function getIp()
{
    ip=`/sbin/ip a | grep 'scope global bond0' | awk '{split($2,a,"/");print a[1]}' | grep -E '^(172\.)' | head -n 1 `
    # if no bond0 interface
    if [[ x"" == x"$ip" ]]; then
        ip=`/sbin/ip a | grep 'inet' | awk '{split($2,a,"/");print a[1]}' | grep -E '^(172\.)' | head -n 1`
    fi
    echo "$ip";
}

#config lepus database server
lepus_db_host="172.21.100.200"
lepus_db_port=3388
lepus_db_user="lepus"
lepus_db_password="lepus"
lepus_db_database="lepus"

#config mysql server
mysql_client="/usr/local/mysql/bin/mysql"
mysql_host=`getIp`
mysql_port=3388
mysql_user="lepusadmin"
mysql_password="lepusadmin"

pt_query_digest="/usr/local/bin/pt-query-digest"
tb_slowquery="mysql_slow_query_review"
tb_slowquery_his=$tb_slowquery"_history"
slowquery_pre="slowquery_"
slowquery_expireday=7

ports=(`ps -ef | grep "mysql" | awk '{for(i=1;i<=NF;i++){split($i,a,"=");if("--port"==a[1]){print a[2]};}}' | sort | uniq`)
#for port  in ${ports[@]}
for mysql_port in `awk -F'=' '{if ($1 ~ "^port"){gsub(/^ *| *$/,"",$2);print $2}}' /etc/my.cnf | sort | uniq`
do
    echo "$mysql_port"

    #config
    slowquery_long_time=1
    slowquery_file=`$mysql_client -h$mysql_host -P$mysql_port -u$mysql_user -p$mysql_password --silent -e "show variables like 'slow_query_log_file'" | awk '{print $2}'`
    echo "$slowquery_file"
    slowquery_dir=`echo "$slowquery_file" | awk -F'/' '{tdir="";for(i=2;i<NF;i++){tdir=tdir"/"$i};print tdir}'`
    echo "$slowquery_dir"

    #config server_id
    lepus_server_id=`$mysql_client -h$lepus_db_host -P$lepus_db_port -u$lepus_db_user -p$lepus_db_password --silent -e "SELECT id FROM db_servers_mysql WHERE HOST=\"$mysql_host\" AND PORT=$mysql_port " $lepus_db_database `
    if [[ x"" == x"$lepus_server_id" ]]; then
	lepus_server_id=1
    fi
    #collect mysql slowquery log into lepus database
    #$pt_query_digest --user=$lepus_db_user --password=$lepus_db_password --port=$lepus_db_port --review h=$lepus_db_host,D=$lepus_db_database,t=mysql_slow_query_review  --history h=$lepus_db_host,D=$lepus_db_database,t=mysql_slow_query_review_history  --no-report --limit=100% --filter=" \$event->{add_column} = length(\$event->{arg}) and \$event->{serverid}=$lepus_server_id " $slowquery_file > /tmp/lepus_slowquery.log
    $pt_query_digest --user=$lepus_db_user --password=$lepus_db_password --port=$lepus_db_port --review h=$lepus_db_host,D=$lepus_db_database,t=$tb_slowquery --history h=$lepus_db_host,D=$lepus_db_database,t=$tb_slowquery_his  --no-report --limit=100% --filter=" \$event->{bytes} = length(\$event->{arg}) and \$event->{serverid}=$lepus_server_id and \$event->{hostname}=\"$mysql_host:$mysql_port\"  " $slowquery_file > /tmp/lepus_slowquery.log

    ##### set a new slow query log ###########
    new_slowquery_log=$slowquery_dir"/"$slowquery_pre"`date '+%Y%m%d_%H%M'`.log"
    echo "$new_slowquery_log"

    #config mysql slowquery
    $mysql_client -h$mysql_host -P$mysql_port -u$mysql_user -p$mysql_password -e "set global slow_query_log=1;set global long_query_time=$slowquery_long_time;set global slow_query_log_file = '$new_slowquery_log';"

    #delete expire log
    /usr/bin/find $slowquery_dir -name "$slowquery_pre*.log" -type f -ctime +$slowquery_expireday -delete;
done
exit 0

####END####

