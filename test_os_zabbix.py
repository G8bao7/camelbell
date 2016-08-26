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
logger = logging.getLogger("root")
path='./include'
sys.path.insert(0,path)
import functions as func
from zabbix_api import zabbixApiClient as zbCli
import thread
from multiprocessing import Process;
import json
import re

dbhost = func.get_config('monitor_server','host')
dbport = func.get_config('monitor_server','port')
dbuser = func.get_config('monitor_server','user')
dbpasswd = func.get_config('monitor_server','passwd')
dbname = func.get_config('monitor_server','dbname')

cpus = int(func.get_config('monitor_os','cpus'))
exeTimeout = int(func.get_config('monitor_os','timeout'))

DEFAULT_ZBSRV_KEY = "DBA"

itemTabs = {}
itemTabs['os'] = 'os_status'
itemTabs['disk'] = 'os_disk'
itemTabs['net'] = 'os_net'
itemTabs['diskio'] = 'os_diskio'


def upd_zbitem_time(stat_item_name, last_stat_time):
    updSql = "UPDATE zabbix_item SET last_stat_time=%s WHERE stat_item_name='%s'" % (last_stat_time, stat_item_name)
    func.mysql_exec(updSql)

'''
# return {item_type:{item_name:item_value}}
'''
def check_os_zabbix(ip, tags, zbSrvItems, zbApis):
    logger.info("check_os_zabbix %s" % (ip))
    stItems = {}
    defZbApi = zbApis.get(DEFAULT_ZBSRV_KEY)
    for zbSrvKey, zbItems in zbSrvItems.iteritems():
        if not zbApis.has_key(zbSrvKey):
	    errMsg = "no zabbix server %s" % (zbSrvKey)
	    print "ERROR", errMsg
	    func.add_alarm(server_id,tags,ip,0,create_time, "os",'check_os_zabbix', 'check_os_zabbix','warning', errMsg)
            continue
        zbApi = zbApis.get(zbSrvKey)
	errItems = []
        for zbItem in zbItems:
            (item_type, stat_item_name, zabbix_item_name, zabbix_item_value_unit, zabbix_server, last_stat_time) = zbItem
            itVal = zbApi.getLastItemValue(ip, zabbix_item_name)
	    if itVal == None:
		itVal = defZbApi.getLastItemValue(ip, zabbix_item_name)
	    if itVal == None:
		errItems.append(zabbix_item_name)
		continue
	    valClock = int(itVal.get("clock"))
	    if not stItems.has_key(item_type):
		stItems[item_type] = {}
	    stItemTypeVals = stItems.get(item_type)
	    if zabbix_item_value_unit > 0:
		srcVal = long(float(itVal.get("value")))
		itVal["value"] = str(srcVal/zabbix_item_value_unit)
	    stItemTypeVals[zbItem] = itVal
	if len(errItems) > 0:
	    logger.error("Error value:%s, %s, %s" % (ip, zbSrvKey, ",".join(errItems)))
    return stItems

def check_hosts(i, hosts, zbSrvItems, zbApis):
    if len(hosts)<=0:
	logger.warn("Warn: no hosts")
        return

    for host in hosts:
    	logger.info(host)
    	(server_id, ip, tags, create_time) = host
    	if not cmp(ip, ''):
	    logger.warn("Error: ip is empty")
            continue
	logger.info(zbSrvItems)
        itemTypeVals = check_os_zabbix(ip, tags, zbSrvItems, zbApis)
	for itemType, itemVals in itemTypeVals.iteritems():
	    if not itemTabs.has_key(itemType):
		errMsg = "no item_type %s on %s" % (itemType, ip)
		print "ERROR", errMsg, itemTabs
		func.add_alarm(server_id,tags,ip,0,create_time, "os",'check_os_zabbix', 'check_os_zabbix','warning', errMsg)
		continue
	    tbName = itemTabs.get(itemType)
	    colNames = ["ip", "tags", "zabbix"]
	    colVals = [ip, tags]
	    if len(itemVals) == 0:
		colVals.append("0")
	    else:
		colVals.append("1")
		for item, statVal in itemVals.iteritems():
		    stat_item_name = item[1]
		    upd_zbitem_time(stat_item_name, statVal.get("clock"))
		    colNames.append(stat_item_name)
		    colVals.append(statVal.get("value"))

	    func.mysql_exec("insert ignore into %s_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from %s where ip='%s';" %(tbName, tbName, ip))

	    insSql = "REPLACE into %s(%s) VALUES ('%s')" % (tbName, ",".join(colNames), "','".join(colVals))
	    logger.info(insSql)
	    func.mysql_exec(insSql)


def main():
    #get os servers list
    zbItems=func.mysql_query("SELECT item_type, stat_item_name, zabbix_item_name, zabbix_item_value_unit, zabbix_server,last_stat_time FROM zabbix_item where item_type='os';")
    zbSrvItems = {}
    for zbItem in zbItems:
	(item_type, stat_item_name, zabbix_item_name, zabbix_item_value_unit, zabbix_server,last_stat_time) = zbItem
        if not zbSrvItems.has_key(zabbix_server):
            zbSrvItems[zabbix_server] = []
        zbSrvItems.get(zabbix_server).append(zbItem)
    #print zbSrvItems 
    zbSectors = ["zabbix_dc", "zabbix_dba"]
    zbApis = {}
    for zbSector in zbSectors:
        zbKey = func.get_config(zbSector,'key')
        zbHost = func.get_config(zbSector,'host')
        zbUser = func.get_config(zbSector,'user')
        zbPasswd = func.get_config(zbSector, 'passwd')
        zbApis[zbKey] = zbCli(zbHost, zbUser, zbPasswd)

    logger.info("check os controller started.")
    servers=func.mysql_query("select id, host,tags, create_time from db_servers_os where is_delete=0 and monitor=1;")
    #servers=func.mysql_query("select id, host,tags,create_time from db_servers_os where host = '172.21.100.71';")
    #servers=func.mysql_query("select id, host,tags,create_time from db_servers_os where host like '172.21.100.%';")
    #servers=func.mysql_query("select id, host,tags,create_time from db_servers_os where host in ('172.18.100.50','172.21.100.71');")
    #servers=func.mysql_query("select id, host,tags,create_time from db_servers_os where host like '172.29.199.%';")
    #servers=func.mysql_query("select id, host,tags,create_time from db_servers_os where host like '172.18.100.%';")
    servers=func.mysql_query("select id, host,tags,create_time from db_servers_os where host like '172.21.100.206';")
    if servers:
        plist = []
	proHostsNum = len(servers)/cpus+1
	logger.info("check os sum:%s, cpus:%s, percpu:%s" % (len(servers), cpus, proHostsNum))
	for i in range(0,len(servers), proHostsNum):
	    proSrvs = servers[i:i+proHostsNum]
	    p = Process(target = check_hosts, args=(i, proSrvs, zbSrvItems, zbApis))
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

	osTabs = ('os_status', 'os_net', 'os_disk', 'os_diskio')
	for osTab in itemTabs.values():
	    func.mysql_exec('DELETE s FROM %s AS s WHERE s.ip NOT IN (SELECT v.host FROM v_monitor_host AS v)' % (osTab))

    else: 
         logger.warning("check os: not found any servers")

    logger.info("check os controller finished.")

if __name__=='__main__':
    main()
