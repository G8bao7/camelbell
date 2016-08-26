#!//bin/env python
#coding:utf-8
import check_os
import check_mysql
import check_oracle
import alarm
import functions as func

def cor():
    hosts = ["172.18.100.11", "172.18.100.10"]
    port = 1588
    user = "lepusmoni"
    passwd = "lepusmonini_2016_o"
    tags = "hk"
    dsn = "order.dhgate.com"
    srvid = 20000
    for host in hosts:
	check_oracle.check_oracle(host,port,dsn, user,passwd,srvid,tags)

    hosts = ["172.18.100.12", "172.18.100.13"]
    port = 1588
    user = "lepusmoni"
    passwd = "lepusmonini_2016_o"
    tags = "hk"
    dsn = "seardb.dhgate.com"
    srvid = 20000
    for host in hosts:
	check_oracle.check_oracle(host,port,dsn, user,passwd,srvid,tags)


def cmy():
    host = "172.18.150.101"
    user = "dhdba"
    passwd = "dhdba_2014"
    tags = "chk"
    ports = [3388, ]
    for port in ports:
	    check_mysql.check_mysql(host,port,user,passwd,10000,tags)

def cos():
    host = '172.18.100.32'
    check_os.check_os_salt(host,tags="",asyncTimeout=3)

def cmy_hk():
    servers = func.mysql_query("select id,host,port,tags from db_servers_mysql where is_delete=0 and monitor=1 and host like '172.18.150.%';")

    exeTimeout = 60
    cnfKey = "monitor_mysql"
    username = func.get_config(cnfKey,'user')
    password = func.get_config(cnfKey,'passwd')

    if servers:
        for row in servers:
            (server_id, host, port, tags) = row
	    print row
            check_mysql.check_mysql(host,port,username,password,server_id,tags)

def main():
    cmy_hk()
    #cor()
    #cmy()

if __name__=='__main__':
     main()
