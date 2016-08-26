#!/usr/bin/env python
#coding:utf-8
import os
import sys
import string
import time
import datetime
import MySQLdb
import pymongo
import bson
import json
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("mongodb")
path='./include'
sys.path.insert(0,path)
import functions as func
from multiprocessing import Process;
from pymongo import MongoClient

def check_mongodb(host,port,user,passwd,server_id,tags):
    print "check_mongodb %s:%s, %s/%s, %s, %s " % (host,port,user,passwd,server_id,tags)
    try:
	'''
        connect = pymongo.Connection(host,int(port))
	print connect
        db = connect['admin'] 
        db.authenticate(user,passwd,mechanism='SCRAM-SHA-1')
	'''
	url = "mongodb://%s:%s@%s:%s/%s?authMechanism=MONGODB-CR" % (user,passwd,host,port,'admin')
	#client = MongoClient(url)
	connect = MongoClient(host,int(port))
        db = connect.admin 
        db.authenticate(user,passwd,mechanism='SCRAM-SHA-1')

        #print connect.admin.command("buildinfo")
	bsCmd = bson.son.SON([('serverStatus', 1), ('repl', 2)])
        serverStatus=connect.admin.command(bsCmd)
        time.sleep(1)
        serverStatus_2=connect.admin.command(bson.son.SON([('serverStatus', 1), ('repl', 2)]))
	#print serverStatus_2
        connect = 1
        ok = int(serverStatus['ok'])
        version = serverStatus['version']
        uptime = serverStatus['uptime']
        connections_current = serverStatus['connections']['current']
        connections_available = serverStatus['connections']['available']
        globalLock_activeClients = serverStatus['globalLock']['activeClients']['total']
        globalLock_currentQueue = serverStatus['globalLock']['currentQueue']['total']

        #dbStats = db.command(bson.son.SON([('dbStats', 1)]))
	keyIndexCounters = "indexCounters"
	if serverStatus.has_key(keyIndexCounters):
	    keyVals = serverStatus.get(keyIndexCounters)
	    indexCounters_accesses = keyVals['accesses']
	    indexCounters_hits = keyVals['hits']
	    indexCounters_misses = keyVals['misses']
	    indexCounters_resets = keyVals['resets']
	    indexCounters_missRatio = keyVals['missRatio']
	else:
	    indexCounters_accesses = 0
	    indexCounters_hits = 0
	    indexCounters_misses = 0
	    indexCounters_resets = 0
	    indexCounters_missRatio = 0
        #cursors_totalOpen = serverStatus['cursors']['totalOpen']
        #cursors_timeOut =  serverStatus['cursors']['timeOut']
	# 2.6 default engine
	keyDur = "dur"
	if serverStatus.has_key(keyDur):
		keyVals = serverStatus.get(keyDur)
		dur_commits = keyVals['commits']
		dur_journaledMB = keyVals['journaledMB']
		dur_writeToDataFilesMB = keyVals['writeToDataFilesMB']
		dur_compression = keyVals['compression']
		dur_commitsInWriteLock = keyVals['commitsInWriteLock']
		dur_earlyCommits = keyVals['earlyCommits']
		dur_timeMs_dt = keyVals['timeMs']['dt']
		dur_timeMs_prepLogBuffer = keyVals['timeMs']['prepLogBuffer']
		dur_timeMs_writeToJournal = keyVals['timeMs']['writeToJournal']
		dur_timeMs_writeToDataFiles = keyVals['timeMs']['writeToDataFiles']
		dur_timeMs_remapPrivateView = keyVals['timeMs']['remapPrivateView']
	else:
		dur_commits = 0
		dur_journaledMB = 0
		dur_writeToDataFilesMB = 0
		dur_compression = 0
		dur_commitsInWriteLock = 0
		dur_earlyCommits = 0
		dur_timeMs_dt = 0
		dur_timeMs_prepLogBuffer = 0
		dur_timeMs_writeToJournal = 0
		dur_timeMs_writeToDataFiles = 0
		dur_timeMs_remapPrivateView = 0
        mem_bits = serverStatus['mem']['bits']
        mem_resident = serverStatus['mem']['resident']
        mem_virtual = serverStatus['mem']['virtual']
        mem_supported = serverStatus['mem']['supported']
        mem_mapped = serverStatus['mem']['mapped']
        mem_mappedWithJournal = serverStatus['mem']['mappedWithJournal']
        network_bytesIn_persecond = int(serverStatus_2['network']['bytesIn']) - int(serverStatus['network']['bytesIn'])
        network_bytesOut_persecond = int(serverStatus_2['network']['bytesOut']) - int(serverStatus['network']['bytesOut'])
        network_numRequests_persecond = int(serverStatus_2['network']['numRequests']) - int(serverStatus['network']['numRequests'])
        opcounters_insert_persecond = int(serverStatus_2['opcounters']['insert']) - int(serverStatus['opcounters']['insert'])
        opcounters_query_persecond = int(serverStatus_2['opcounters']['query']) - int(serverStatus['opcounters']['query'])
        opcounters_update_persecond = int(serverStatus_2['opcounters']['update']) - int(serverStatus['opcounters']['update'])
        opcounters_delete_persecond = int(serverStatus_2['opcounters']['delete']) - int(serverStatus['opcounters']['delete'])
        opcounters_command_persecond = int(serverStatus_2['opcounters']['command']) - int(serverStatus['opcounters']['command'])

        #replset
        try:
            repl=serverStatus['repl']
            setName=repl['setName']
            replset=1
            if repl['ismaster']== True:
                repl_role='master'
                repl_role_new='m' 
            elif repl['secondary']== True:
                repl_role='secondary'
                repl_role_new='s'
            else:
                repl_role='arbiterOnly'
                repl_role_new='a' 
        except:
            replset=0
            repl_role='master'
            repl_role_new='m'
            pass

        ##################### insert data to mysql server#############################
        sql = "insert into mongodb_status(server_id,host,port,tags,connect,replset,repl_role,ok,uptime,version,connections_current,connections_available,globalLock_currentQueue,globalLock_activeClients,indexCounters_accesses,indexCounters_hits,indexCounters_misses,indexCounters_resets,indexCounters_missRatio,dur_commits,dur_journaledMB,dur_writeToDataFilesMB,dur_compression,dur_commitsInWriteLock,dur_earlyCommits,dur_timeMs_dt,dur_timeMs_prepLogBuffer,dur_timeMs_writeToJournal,dur_timeMs_writeToDataFiles,dur_timeMs_remapPrivateView,mem_bits,mem_resident,mem_virtual,mem_supported,mem_mapped,mem_mappedWithJournal,network_bytesIn_persecond,network_bytesOut_persecond,network_numRequests_persecond,opcounters_insert_persecond,opcounters_query_persecond,opcounters_update_persecond,opcounters_delete_persecond,opcounters_command_persecond) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"       
        param = (server_id,host,port,tags,connect,replset,repl_role,ok,uptime,version,connections_current,connections_available,globalLock_currentQueue,globalLock_activeClients,indexCounters_accesses,indexCounters_hits,indexCounters_misses,indexCounters_resets,indexCounters_missRatio,dur_commits,dur_journaledMB,dur_writeToDataFilesMB,dur_compression,dur_commitsInWriteLock,dur_earlyCommits,dur_timeMs_dt,dur_timeMs_prepLogBuffer,dur_timeMs_writeToJournal,dur_timeMs_writeToDataFiles,dur_timeMs_remapPrivateView,mem_bits,mem_resident,mem_virtual,mem_supported,mem_mapped,mem_mappedWithJournal,network_bytesIn_persecond,network_bytesOut_persecond,network_numRequests_persecond,opcounters_insert_persecond,opcounters_query_persecond,opcounters_update_persecond,opcounters_delete_persecond,opcounters_command_persecond)
        func.mysql_exec(sql,param)
        role='m'
        func.update_db_status_init(server_id,repl_role_new,version,host,port,tags)

    except Exception, e:
        logger_msg="check mongodb %s:%s,%s/%s : %s" %(host,port,user,passwd,e)
        logger.warning(logger_msg)

        try:
            connect=0
            sql="replace into mongodb_status(server_id,host,port,tags,connect) values(%s,%s,%s,%s,%s)"
            param=(server_id,host,port,tags,connect)
            func.mysql_exec(sql,param)

        except Exception, e:
            logger.error(e)
            sys.exit(1)
        finally:
            sys.exit(1)

    finally:
        func.check_db_status(server_id,host,port,tags,'mongodb')   
        sys.exit(1)



def main():

    func.mysql_exec("replace into mongodb_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from mongodb_status;",'')
    func.mysql_exec('delete from mongodb_status;','')

    #get mongodb servers list
    servers = func.mysql_query('select id,host,port,tags from db_servers_mongodb where is_delete=0 and monitor=1;')

    logger.info("check mongodb controller started.")

    exeTimeout = 60
    cnfKey = "monitor_mongodb"
    username = func.get_config(cnfKey,'user')
    password = func.get_config(cnfKey,'passwd')
    min_interval = func.get_option('min_interval')

    if servers:
         plist = []
         for row in servers:
             (server_id, host, port, tags)=row
             p = Process(target = check_mongodb, args = (host,port,username,password,server_id,tags))
             plist.append(p)
             p.start()

         for p in plist:
             p.join()

    else:
         logger.warning("check mongodb: not found any servers")

    func.mysql_exec('update mongodb_status set connect=0,create_time=now() where create_time<date_sub(now(), interval %s second)' % (min_interval))
    func.mysql_exec('DELETE ds FROM mongodb_status AS ds, (SELECT s.id,d.host FROM mongodb_status AS s LEFT JOIN db_servers_mongodb AS d  ON d.is_delete=0 AND d.monitor=1 AND s.host=d.host AND s.port=d.port HAVING d.`host` IS NULL) AS t WHERE ds.id=t.id')
    func.mysql_exec('DELETE ds FROM db_status AS ds, (SELECT s.id,d.host FROM db_status AS s LEFT JOIN db_servers_mongodb AS d ON d.is_delete=0 AND d.monitor=1 AND s.host=d.host AND s.port=d.port  WHERE db_type="mongodb" HAVING d.`host` IS NULL) AS t WHERE ds.id=t.id')

    logger.info("check mongodb controller finished.")


if __name__=='__main__':
    main()
