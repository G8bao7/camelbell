#!/usr/bin/env python
#coding:utf-8
import os
import sys
import string
import time
import re
import datetime
import MySQLdb
path='./include'
sys.path.insert(0,path)
import functions as func
from multiprocessing import Process;

def check_mysql_bigtable(no, host,port,username,password,server_id,tags,bigtable_size):
    try:
	print "[BBQ] check_mysql_bigtable %s %s:%s"%(no, host,port)
        conn=MySQLdb.connect(host=host,user=username,passwd=password,port=int(port),connect_timeout=2,charset='utf8')
        curs=conn.cursor()
        conn.select_db('information_schema')
        try:
	    # datadir disk size
	    varis = curs.execute("show global variables like 'datadir'")
	    dataDir = (curs.fetchone())[1]
	    disk_size_m = -1
	    saltCmd = "du -sm %s" % (dataDir.rstrip("/"))
	    jobID = func.exeSaltAsyncCmd(host, saltCmd)
	    time.sleep(3)
	    for i in range(0, 3):
		saltRess = func.getSaltJobByID(host, jobID)
		if saltRess != None:
		    sizess = saltRess.split("\n")
		    svals = (re.sub(r'\s+', ' ', sizess[len(sizess)-1])).split()
		    disk_size_m = int(svals[0])
		    break
		else:
		    print i, host, jobID, saltRess
	    updSql = "update mysql_status set disk_size_m=%s where host='%s' and port=%s" % (disk_size_m, host, port)
	    func.mysql_exec(updSql)

	    # bigtable
	    bigtable=curs.execute("SELECT table_schema as 'DB',table_name as 'TABLE',IFNULL(ROUND(( data_length + index_length ) / ( 1024 * 1024 ), 2),0) as 'total_length' , table_comment as COMMENT FROM information_schema.TABLES WHERE IFNULL(ROUND(( data_length + index_length ) / ( 1024 * 1024 ), 2),0) >= %s ORDER BY total_length DESC ;" % (bigtable_size))
	    lines = curs.fetchall()
	    for line in lines:
		updSqls = []
		updSqls.append("insert into mysql_bigtable_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),8) from mysql_bigtable where host='%s' and port=%s" % (host,port))
		updSqls.append("delete from mysql_bigtable  where host='%s' and port=%s" % (host,port))
		sql="insert into mysql_bigtable(server_id,host,port,tags,db_name,table_name,table_size,table_comment) values('%s','%s','%s','%s','%s','%s','%s','%s');" % (server_id,host,port,tags, line[0], line[1], line[2], line[3])
		updSqls.append(sql)

		#func.mysql_exec_many(updSqls)
		for updSql in updSqls:
		    func.mysql_exec(updSql)

	except MySQLdb.Error,e:
           print "warn %s:%s,%s,%s" % (host,port,username,password)
	   print e

        finally:
           curs.close()
           conn.close()

    except MySQLdb.Error,e:
        pass
        print "Mysql Error %d: %s" %(e.args[0],e.args[1])


def check_mysql_bigtable2(no, host,port,username,password,server_id,tags,bigtable_size):
    try:
	print "[BBQ] check_mysql_bigtable %s %s:%s"%(no, host,port)
        conn=MySQLdb.connect(host=host,user=username,passwd=password,port=int(port),connect_timeout=2,charset='utf8')
        curs=conn.cursor()
        conn.select_db('information_schema')
        try:
	    disk_size_m = -1
	    # get datadir
	    curs.execute("show global variables like 'datadir'")
	    dataDir = (curs.fetchone())[1]

	    # get database
	    curs.execute("show databases")
	    for line in curs.fetchall():
		dbName = line[0]
		saltCmd = "du -ch %s/%s/*" % (dataDir.rstrip("/"), dbName)
		jobID = func.exeSaltAsyncCmd(host, saltCmd)
		time.sleep(3)
		saltRess = func.getSaltJobByID(host, jobID)
		if saltRess is None:
		    break
		
		sizess = saltRess.split("\n")
		svals = (re.sub(r'\s+', ' ', sizess[len(sizess)-1])).split()
		disk_size_m = int(svals[0])
		updSql = "update mysql_status set disk_size_m=%s where host='%s' and port=%s" % (disk_size_m, host, port)
		func.mysql_exec(updSql)

	    # bigtable
	    bigtable=curs.execute("SELECT table_schema as 'DB',table_name as 'TABLE',IFNULL(ROUND(( data_length + index_length ) / ( 1024 * 1024 ), 2),0) as 'total_length' , table_comment as COMMENT FROM information_schema.TABLES WHERE IFNULL(ROUND(( data_length + index_length ) / ( 1024 * 1024 ), 2),0) >= %s ORDER BY total_length DESC ;" % (bigtable_size))
	    lines = curs.fetchall()
	    for line in lines:
		updSqls = []
		updSqls.append("insert into mysql_bigtable_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),8) from mysql_bigtable where host='%s' and port=%s" % (host,port))
		updSqls.append("delete from mysql_bigtable  where host='%s' and port=%s" % (host,port))
		sql="insert into mysql_bigtable(server_id,host,port,tags,db_name,table_name,table_size,table_comment) values('%s','%s','%s','%s','%s','%s','%s','%s');" % (server_id,host,port,tags, line[0], line[1], line[2], line[3])
		updSqls.append(sql)

		#func.mysql_exec_many(updSqls)
		for updSql in updSqls:
		    func.mysql_exec(updSql)

	except MySQLdb.Error,e:
           print "warn %s:%s,%s,%s" % (host,port,username,password)
	   print e

        finally:
           curs.close()
           conn.close()

    except MySQLdb.Error,e:
        pass
        print "Mysql Error %d: %s" %(e.args[0],e.args[1])


def main():
    #get mysql servers list
    servers = func.mysql_query('select m.id,m.host,m.port,m.tags,m.bigtable_size from db_servers_mysql as m , mysql_status as s where m.is_delete=0 and m.monitor=1 and m.bigtable_monitor=1 and m.host=s.host and m.port=s.port and s.role="master" order by host;')

    #++ guoqi
    exeTimeout = 20
    cnfKey = "monitor_mysql"
    username = func.get_config(cnfKey,'user')
    password = func.get_config(cnfKey,'passwd')

    if servers:
        print("%s: check mysql bigtable controller started." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),));
        plist = []
	no = 1
        for row in servers:
            (server_id, host, port, tags, bigtable_size) = row
	    check_mysql_bigtable(no,host,port,username,password,server_id,tags,bigtable_size)
	    no += 1
	'''
            p = Process(target = check_mysql_bigtable, args = (no,host,port,username,password,server_id,tags,bigtable_size))
            plist.append(p)
	    no += 1
        for p in plist:
            p.start()
        time.sleep(exeTimeout)
        for p in plist:
            p.terminate()
        for p in plist:
            p.join()
	'''
        print("%s: check mysql bigtable controller finished." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),))
                     

if __name__=='__main__':
    main()
