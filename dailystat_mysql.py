#!/usr/bin/env python
#coding:utf-8
import os
import sys
import string
import time
import re
import datetime
import MySQLdb
(pathAbs, scName) = os.path.split(os.path.abspath(sys.argv[0]))
path='%s/include' % (pathAbs)
sys.path.insert(0,path)
#path='./include'
#sys.path.insert(0,path)
import functions as func
from multiprocessing import Process;

igDbs = ['information_schema']

def stat_mysql_summary():
    print("%s: stat_mysql_summary started." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),));
    statSql = '''
    REPLACE INTO daily_mysql
	(server_id, host, port, tags, db_type, disk_size_m
	, tps_avg, tps_min, tps_max, qps_avg, qps_min, qps_max,stat_date, create_time)
	
    SELECT server_id, HOST, PORT, tags, 'mysql', MAX(disk_size_m),
      CEIL(AVG(IF(com_insert_persecond>0,com_insert_persecond,0)
		+ IF(com_update_persecond>0,com_update_persecond,0)
		+ IF(com_delete_persecond>0,com_delete_persecond,0))),
      CEIL(MIN(IF(com_insert_persecond>0,com_insert_persecond,0)
		+ IF(com_update_persecond>0,com_update_persecond,0)
		+ IF(com_delete_persecond>0,com_delete_persecond,0))),
      CEIL(MAX(IF(com_insert_persecond>0,com_insert_persecond,0)
		+ IF(com_update_persecond>0,com_update_persecond,0)
		+ IF(com_delete_persecond>0,com_delete_persecond,0))),
      MIN(IF(com_select_persecond>0,com_select_persecond,0)),
      MAX(IF(com_select_persecond>0,com_select_persecond,0)),
      CEIL(AVG(IF(com_select_persecond>0,com_select_persecond,0))),
      DATE(create_time),NOW()
    FROM mysql_status_history
    WHERE create_time>=DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND create_time<CURDATE()
    GROUP BY DATE(create_time),HOST,PORT
                '''
    func.mysql_exec(statSql)

# slow query
def stat_mysql_slowquery():
    print("%s: stat_mysql_slowquery started." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),));
    statSlowSql = '''
        REPLACE INTO mysql_slow_query_review_summary
            (id, serverid_max, hostname_max, db_max, user_max, checksum, ts_min, ts_max, ts_cnt,
             Query_time_pct_95, Lock_time_pct_95, Rows_sent_pct_95, Rows_examined_pct_95)
        SELECT  MIN(id), serverid_max, hostname_max, db_max, user_max, checksum, MIN(ts_min), MAX(ts_max), SUM(ts_cnt),
          MAX(Query_time_pct_95), MAX(Lock_time_pct_95), MAX(Rows_sent_pct_95), MAX(Rows_examined_pct_95)
        FROM mysql_slow_query_review_history
        GROUP BY hostname_max,db_max,CHECKSUM ORDER BY NULL;
        '''
    func.mysql_exec(statSlowSql)

def check_mysql_tablespace(no, host,port,username,password,server_id,tags,bigtable_size):
    try:
        conn=MySQLdb.connect(host=host,user=username,passwd=password,port=int(port),connect_timeout=2,charset='utf8')
        curs=conn.cursor()
        conn.select_db('information_schema')
        try:
	    disk_size_m = 0
	    saveDbs = []
	    bigTables = []
	    # get datadir
	    curs.execute("show global variables like 'datadir'")
	    dataDir = (curs.fetchone())[1]

	    # get database
	    curs.execute("show databases")
	    for line in curs.fetchall():
		dbName = line[0]
		tbnum = 0
		dbsize_m = 0
		if igDbs.count(dbName) > 0:
		    continue
		saltCmd = "du -cm %s/%s/* 2>/dev/null" % (dataDir.rstrip("/"), dbName)
		jobID = func.exeSaltAsyncCmd(host, saltCmd)
		time.sleep(3)
		saltRess = func.getSaltJobByID(host, jobID)
		if saltRess is None:
		    break
		slines = saltRess.split("\n")
		for sline in slines:
		    (dsize_m, dfile) = re.sub(r'\s+', ' ', sline).split()
		    dsize_m = int(dsize_m)
		    tbfile = dfile.split("/")[-1]
		    if not cmp("total", tbfile):
			disk_size_m += dsize_m
			continue
		    elif re.search("^db.", tbfile):
			continue

		    tbName = tbfile.split(".")[0]
		    if re.search("frm$", tbfile):
			tbnum += 1
		    else:
			dbsize_m += dsize_m
			if dsize_m > bigtable_size:
			    bigTables.append([dbName, tbName, dsize_m])
		saveDbs.append([dbName, host, port, tbnum, dbsize_m])
	   
	    print "%s:%s, disk_size_m %s, bigtbs %s" % (host,port, disk_size_m, len(bigTables))

	    # databases
	    func.mysql_exec("delete from mysql_databases  where db_ip='%s' and db_port=%s" % (host,port))
	    if len(saveDbs) >0:
		insVals = []
		insSql="INSERT INTO mysql_databases(db_name, db_ip, db_port, tb_count, data_size_m) values(%s,%s,%s,%s,%s);"
		func.mysql_exec(insSql, saveDbs)

	    # bigtable
	    func.mysql_exec("delete from mysql_bigtable  where host='%s' and port=%s" % (host,port))
	    if len(bigTables) >0:
		insVals = []
		for bigTable in bigTables:
		    (dbName, tbName, dsize_m) = bigTable
		    insVals.append([server_id,host,port,tags, dbName, tbName, dsize_m, ''])
		insSql="insert into mysql_bigtable(server_id,host,port,tags,db_name,table_name,table_size,table_comment) values(%s,%s,%s,%s,%s,%s,%s,%s);"
		func.mysql_exec(insSql, insVals)

	except MySQLdb.Error,e:
           print "warn %s:%s,%s,%s" % (host,port,username,password)
	   print e

        finally:
           curs.close()
           conn.close()

    except MySQLdb.Error,e:
        pass
        print "Mysql Error %d: %s" %(e.args[0],e.args[1])

def stat_mysql_tablespace():
    print("%s: check mysql bigtable controller started." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),));
    # get mysql servers list
    servers = func.mysql_query('select m.id,m.host,m.port,m.tags,m.bigtable_size from db_servers_mysql as m , mysql_status as s where m.is_delete=0 and m.monitor=1 and m.bigtable_monitor=1 and m.host=s.host and m.port=s.port and s.role="master" order by host;')

    cnfKey = "monitor_mysql"
    username = func.get_config(cnfKey,'user')
    password = func.get_config(cnfKey,'passwd')

    if servers:
	no = 1
        for row in servers:
            (server_id, host, port, tags, bigtable_size) = row
	    if re.search("^172.30",host):
		continue
	    print "%s/%s %s, %s" % (no, len(servers), host, port)
	    check_mysql_tablespace(no,host,port,username,password,server_id,tags,bigtable_size)
	    no += 1
    print("%s: check mysql bigtable controller finished." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),))
                     
def main():
    stat_mysql_summary()
    stat_mysql_slowquery()
    stat_mysql_tablespace()

if __name__=='__main__':
    main()
