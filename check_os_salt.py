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
from multiprocessing import Process;
import json
import re

dbhost = func.get_config('monitor_server','host')
dbport = func.get_config('monitor_server','port')
dbuser = func.get_config('monitor_server','user')
dbpasswd = func.get_config('monitor_server','passwd')
dbname = func.get_config('monitor_server','dbname')

saltAsyncTimeout = 30
exeTimeout = 300

def check_os_salt(ip,tags, asyncTimeout=saltAsyncTimeout):
    print "check_os_salt %s start, asyncTimeout:%s " % (ip, asyncTimeout)
    if not func.checkSaltKey(ip):
        print " %s salt failed" % (ip)
        insSql = "replace into %s.os_status(ip,tags) VALUES('%s', '%s')" % (dbname, ip, tags)
        func.mysql_exec(insSql)
        return 

    # check last statistics time
    circleSecds = exeTimeout
    qSql = "select create_time from %s.os_status where ip='%s' and tags='%s' and salt=1 and create_time>date_sub(now(), interval %s second); " % (dbname, ip, tags, circleSecds)
    lines = func.mysql_query(qSql)
    if lines != 0 and len(lines) > 0:
	print "%s, %s,don't do check, last time %s" % (ip, tags, lines[0])
        return

    osStats = {}
    
    '''
    ip,snmp,tags,hostname,kernel,os,system_date,system_uptime,process,load_1,load_5,load_15
    '''
    osStats["salt"] = "1"

    saltCmds = {}
    cmdHost = "/bin/hostname"
    saltCmds[cmdHost] = {}
    cmdKernal = "uname -r"
    saltCmds[cmdKernal] = {}    
    cmdOsVers = "cat /etc/redhat-release"
    saltCmds[cmdOsVers] = {}
    cmdTime = "/bin/date +'%Y-%m-%d %H:%M:%S'"
    saltCmds[cmdTime] = {}
    cmdRuntime = "awk '{print $1}' /proc/uptime"
    saltCmds[cmdRuntime] = {}
    cmdProcNum = "ps -ef|wc -l" 
    saltCmds[cmdProcNum] = {}
    cmdUptime = "/usr/bin/uptime"
    saltCmds[cmdUptime] = {}
    cmdVmstat = "/usr/bin/vmstat 1 2| tail -n 1"
    saltCmds[cmdVmstat] = {}
    cmdIostat = "iostat -dmx 1 2"
    saltCmds[cmdIostat] = {}
    cmdFree = "/usr/bin/free"
    saltCmds[cmdFree] = {}
    cmdDf = "df"
    saltCmds[cmdDf] = {}
    cmdSar = "sar -n DEV 1 2"
    saltCmds[cmdSar] = {}
    cmdIfname = "grep '%s' /etc/sysconfig/network-scripts/ifcfg-*| awk -F'-' '{split($NF,a,\":\");print a[1]}' " % (ip)
    saltCmds[cmdIfname] = {}

    KEY_JOBID = "jobid"
    SALT_RES = "res"
    for saltCmd in saltCmds.keys():
	jobID = func.exeSaltAsyncCmd(ip, saltCmd)
	saltCmds[saltCmd][KEY_JOBID] = jobID
    logger.info("%s start salt job" % (ip))
    for i in range(0, asyncTimeout, 3):
	exeOk = True
	for saltCmd, saltVals in saltCmds.iteritems():
	    if saltVals.has_key(SALT_RES):
		continue
	    jobID = saltVals.get(KEY_JOBID)
	    if jobID == None:
		saltVals[SALT_RES] = None
	    else:
		saltRes = func.getSaltJobByID(ip, jobID)
		if saltRes != None:
		    saltVals[SALT_RES] = saltRes
		else:
		    exeOk = False
	if exeOk:
	    break
	else:
	    time.sleep(3)
    logger.info("%s Finish salt cmd" % (ip))

    os_hostname = saltCmds.get(cmdHost).get(SALT_RES)
    osStats["hostname"] = os_hostname
    kernal_version = saltCmds.get(cmdKernal).get(SALT_RES)
    osStats["kernel"] = kernal_version
    os_version = saltCmds.get(cmdOsVers).get(SALT_RES)
    osStats["os"] = os_version
    curTime = saltCmds.get(cmdTime).get(SALT_RES)
    osStats["system_date"] = curTime
    system_uptime = int(float(saltCmds.get(cmdRuntime).get(SALT_RES)))
    osStats["system_uptime"] = system_uptime
    procs = saltCmds.get(cmdProcNum).get(SALT_RES)
    osStats["process"] = int(procs)
    #  18:25:12 up 981 days,  3:29,  3 users,  load average: 0.92, 0.44, 0.37
    upTime = saltCmds.get(cmdUptime).get(SALT_RES)
    tvals = re.sub(r'\s+', ' ', upTime).split("load average:")
    (load_1, load_5, load_15) = re.sub(r',', '', tvals[1].strip()).split()
    osStats["load_1"] = float(load_1)
    osStats["load_5"] = float(load_5)
    osStats["load_15"] = float(load_15)
    
    '''
    cpu_user_time,cpu_system_time,cpu_idle_time,swap_total,swap_avail,mem_total,mem_used,mem_free,mem_shared,mem_buf
fered,mem_cached,mem_usage_rate,mem_available,disk_io_reads_total,disk_io_writes_total,net_in_bytes_total,net_out_bytes_total
    '''
    #procs -----------memory---------- ---swap-- -----io---- --system-- -----cpu-----
    # r  b   swpd   free   buff  cache   si   so    bi    bo   in   cs us sy id wa st
    # 0  0 110452 432736 438964 42745196    0    0    24   516    0    0  1  0 98  0  0    
    # 4  0 110452 433108 438964 42745196    0    0     0 27280 2767 8917  2  1 97  0  0
    vms = saltCmds.get(cmdVmstat).get(SALT_RES)
    tvals = re.sub(r'\s+', ' ', vms).split()
    cpu_user_time = int(tvals[12])
    cpu_system_time = int(tvals[13])
    cpu_idle_time = int(tvals[14])
    osStats["cpu_user_time"] = cpu_user_time
    osStats["cpu_system_time"] = cpu_system_time
    osStats["cpu_idle_time"] = cpu_idle_time

    #            total       used       free     shared    buffers     cached
    #Mem:      49376364   47264788    2111576       1628     261000    5218492
    #-/+ buffers/cache:   41785296    7591068
    #Swap:     16383992      89000   16294992
    frees = saltCmds.get(cmdFree).get(SALT_RES).split("\n")
    for i in range(1, 4):
        ttvals = re.sub(r'\s+', ' ', frees[i]).split()
        if i == 1:
            (title, mem_total, mem_used, mem_free, mem_shared, mem_buffered, mem_cached) = ttvals
        elif i == 3:
            (swap_pre, swap_total, swap_used, swap_avail) = ttvals
    osStats["swap_total"] = long(swap_total)
    osStats["swap_avail"] = long(swap_avail)
    osStats["mem_total"] = long(mem_total)
    osStats["mem_used"] = long(mem_used)
    osStats["mem_free"] = long(mem_free)
    osStats["mem_shared"] = long(mem_shared)
    osStats["mem_buffered"] = long(mem_buffered)
    osStats["mem_cached"] = long(mem_cached)
    osStats["mem_usage_rate"] = float(osStats.get("mem_used")*100/osStats.get("mem_total"))
    osStats["mem_available"] = float(osStats.get("mem_free")*100/osStats.get("mem_total"))

    '''
    net_in_bytes_total,net_out_bytes_total
    '''
    #Average:        IFACE   rxpck/s   txpck/s    rxkB/s    txkB/s   rxcmp/s   txcmp/s  rxmcst/s
    #Average:          em1      5.03      0.00      0.42      0.00      0.00      0.00      2.01
    #Average:          em2   9198.99  11337.19   1306.44  12159.31      0.00      0.00      2.01
    #Average:        bond0   9204.02  11337.19   1306.86  12159.31      0.00      0.00      4.02
    ifName = saltCmds.get(cmdIfname).get(SALT_RES)
    sars = saltCmds.get(cmdSar).get(SALT_RES).split("\n")
    sarKeys = []
    saveNets = {}
    iface = {}
    for sar in sars:
        if re.search("^Average", sar):
            ttvals = re.sub(r'\s+', ' ', sar).split()
            if re.search("IFACE", sar):
                sarKeys = ttvals
            else:
                if not cmp(ifName, ttvals[1]):
		    for j in range(1, len(sarKeys)):
			iface[sarKeys[j]] = ttvals[j]
                    saveNet = {}
                    saveNet["if_descr"] = ifName
                    saveNet["in_bytes"] = iface.get("rxkB/s")
                    saveNet["out_bytes"] = iface.get("txkB/s")
                    saveNets[ifName] = saveNet
    if len(iface) > 0:
	osStats["net_in_bytes_total"] = float(iface.get("rxkB/s"))
	osStats["net_out_bytes_total"] = float(iface.get("txkB/s"))
    else:
	print iface
    
    '''
    disk_io_reads_total,disk_io_writes_total
    '''
    #Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await  svctm  %util
    #sda               0.00 14164.00    0.00 2063.00     0.00    63.23    62.77     0.39    0.19   0.06  11.90
    #sdc               0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00   0.00   0.00
    saltIostat = saltCmds.get(cmdIostat).get(SALT_RES).split("\n")
    iostats = {}
    iovalss = []
    saveIostats = {}
    for i in range(len(saltIostat)-1,0,-1):
        tline = saltIostat[i]
        if not cmp("", tline):
            continue
        ttvals = re.sub(r'\s+', ' ', tline).split()
        deviceName = ttvals[0]
        if not cmp("Device:", deviceName):
            for iovals in iovalss:
                tDname = iovals[0]
                tDvals = {}
                for j in range(0, len(ttvals)):
                    tDvals[ttvals[j]] = iovals[j]
                
                iostats[tDname] = tDvals
                saveIostat = {}
                saveIostat["fdisk"] = tDvals.get("Device:")
                saveIostat["disk_io_reads"] = float(tDvals.get("r/s"))
                saveIostat["disk_io_writes"] = float(tDvals.get("w/s"))
                saveIostat["disk_io_read_mb"] = float(tDvals.get("rMB/s"))
                saveIostat["disk_io_write_mb"] = float(tDvals.get("wMB/s"))
                saveIostat["disk_io_util"] = float(tDvals.get("%util"))
                saveIostats[saveIostat.get("fdisk")] = saveIostat
            break
        else:
            iovalss.append(ttvals)
    disk_io_reads_total = 0
    disk_io_writes_total = 0
    for saveIostat in saveIostats.itervalues():
        disk_io_reads_total += saveIostat.get("disk_io_reads")
        disk_io_writes_total += saveIostat.get("disk_io_writes")
    osStats["disk_io_reads_total"] = disk_io_reads_total
    osStats["disk_io_writes_total"] = disk_io_writes_total

    '''
    disk info
    '''
    #Filesystem     1K-blocks       Used Available Use% Mounted on
    #/dev/sda3      1422936368 696619164 654036168  52% /
    saltDfs = saltCmds.get(cmdDf).get(SALT_RES).split("\n")
    saveDisks = {}
    igMounts = ["/dev/shm", "/boot", "/home", "/var"]
    mysqlDevice = None
    mysqlDisk = None
    for df in saltDfs[1:]:
	dfVals = re.sub(r'\s+', ' ', df).split()
	if len(dfVals) != 6:
	    print "Error df: %s:%s,%s" % (ip, cmdDf, saltDfs)
	    continue
        (dName, dTotal, dUsed, dAvail, dUsePct, dMount) = dfVals
        if dTotal <= 10**7:
            continue

        saveDisk = {}
        saveDisk["device"] = dName
        saveDisk["mounted"] = dMount
        saveDisk["total_size"] = long(dTotal)
        saveDisk["used_size"] = long(dUsed)
        saveDisk["avail_size"] = long(dAvail)
        saveDisk["used_rate"] = int(dUsePct.rstrip("%"))
        muts = func.exeSaltCmd(ip, "/bin/mount -l | egrep '^%s'" % (dName))
        ttvals = re.sub(r'\s+', ' ', muts).split()
        dFileSystem = ttvals[4]
        saveDisk["file_system"] = dFileSystem

        if re.search("^/mysql",dMount) or ((not cmp("/", dMount)) and mysqlDisk == None):
            mysqlDisk = saveDisk

        #if saveDisk.get("used_rate") > 90 or ( saveDisk.get("used_rate") <= 90 and igMounts.count(dMount) == 0):
        #if saveDisk.get("used_rate") > 80 and igMounts.count(dMount) == 0:
        if igMounts.count(dMount) == 0:
            saveDisks[dName] = saveDisk
        
    diskUsed = mysqlDisk.get("used_size")
    diskAvail = mysqlDisk.get("avail_size")

    ### save status
    saveVals = {}
    saveVals["os_status"] = {ip:osStats}
    saveVals["os_net"] = saveNets
    saveVals["os_disk"] = saveDisks
    saveVals["os_diskio"] = saveIostats
    logger.info(saveVals)
    for tbName, statVal in saveVals.iteritems():
	func.mysql_exec("insert into %s.%s_history select *, LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from %s.%s where ip='%s';" %(dbname,tbName, dbname, tbName, ip),'')
	func.mysql_exec("delete from %s.%s where ip='%s';" %(dbname, tbName, ip),'')
	for sKeyVal in statVal.itervalues():
	    sKeys = ["ip", "tags"]
	    sValues = [ip, tags]
	    for key, value in sKeyVal.iteritems():
		sKeys.append(key)
		sValues.append(str(value))
	    insSql = "insert into %s.%s(%s) VALUES ('%s')" % (dbname, tbName, ",".join(sKeys), "','".join(sValues))
	    func.mysql_exec(insSql)

def check_hosts(i,hosts):
    global saltAsyncTimeout
    if len(hosts)<=0:
	return
    curAsyncTimeout = exeTimeout/len(hosts) - 1
    asyncTimeout = saltAsyncTimeout
    if curAsyncTimeout < asyncTimeout:
    	asyncTimeout = curAsyncTimeout

    for host in hosts:
	logger.info("check os %s" % (",".join(host)))
	(ip, tags) = host
	if ip <> '':
	    check_os_salt(ip, tags, asyncTimeout)

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
