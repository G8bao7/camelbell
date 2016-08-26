#!//bin/env python
#coding:utf-8
import os
import sys
import string
import time
import datetime
import MySQLdb
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("os")
path='./include'
sys.path.insert(0,path)
import functions as func
import thread
from multiprocessing import Process
import json
import re

dbhost = func.get_config('monitor_server','host')
dbport = func.get_config('monitor_server','port')
dbuser = func.get_config('monitor_server','user')
dbpasswd = func.get_config('monitor_server','passwd')
dbname = func.get_config('monitor_server','dbname')

def check_os_snmpwalk(ip,community,filter_os_disk,tags):
    command="sh check_os_snmpwalk.sh"
    try :
        os.system("%s %s %s %s %s %s %s %s %s %s"%(command,ip,dbhost,dbport,dbuser,dbpasswd,dbname,community,filter_os_disk,tags))
   
    except Exception, e:
            print e
            sys.exit(1)
    finally:
            sys.exit(1)

def check_hosts(i,hosts):
    if len(hosts)<=0:
	return

    for host in hosts:
	logger.info("check os %s" % (",".join(host)))
	(ip, tags) = host
	if ip <> '':
	    check_os_snmpwalk(ip, tags)

def main():
    #get os servers list
    cpus = 12
    servers=func.mysql_query("select host,tags from db_servers_os where is_delete=0 and monitor=1;")
    logger.info("check os controller started.")
    if servers:
        plist = []
	proHostsNum = len(servers)/cpus+1
	logger.info("check os sum:%s, cpus:%s, percpu:%s" % (len(servers), cpus, proHostsNum))
	for i in range(0,len(servers), proHostsNum):
	    proSrvs = servers[i:i+proHostsNum]
	    p = Process(target = check_hosts, args=(i, proSrvs))
	    plist.append(p)
	intervals = exeTimeout/len(plist)
	if intervals <= 0:
	    intervals = 1

	for p in plist:
	    p.start()
	    #time.sleep(intervals)
	    time.sleep(1)
        
	for p in plist:
	    p.join(timeout=10)

    else: 
         logger.warning("check os: not found any servers")

    logger.info("check os controller finished.")

if __name__=='__main__':
    main()
