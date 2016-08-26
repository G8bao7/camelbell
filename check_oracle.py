#!//bin/env python
#coding:utf-8
import os
import sys
import string
import time
import datetime
import MySQLdb
import cx_Oracle
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("oracle")
path='./include'
sys.path.insert(0,path)
import functions as func
import camelbell_oracle as oracle
from multiprocessing import Process;


def check_oracle(host,port,dsn,username,password,server_id,tags):
    url = "%s:%s/%s" % (host, port, dsn)

    logger_msg = "[BBQ]begin check oracle %s " %(url)
    logger.info(logger_msg)
    retry = 4
    conn = None
    for i in range(1,retry):
	try:
	    logger_msg="[BBQ] oracle connect %s retry [%s]" %(url, i)
	    logger.info(logger_msg)
	    conn=cx_Oracle.connect(username,password,url) #获取connection对象
	    break
	except Exception, e:
	    logger_msg="[BBQ] oracle connect %s, %s" %(url,str(e).strip('\n'))
	    logger.warning(logger_msg)
	    conn = None
	continue

    func.check_db_status(server_id,host,port,tags,'oracle')   
    if conn == None:
        try:
            connect=0
            sql="replace into oracle_status(server_id,host,port,tags,connect) values(%s,%s,%s,%s,%s)"
            param=(server_id,host,port,tags,connect)
            func.mysql_exec(sql,param)
        except Exception, e:
            logger.error(str(e).strip('\n'))
            sys.exit(1)
        finally:
            sys.exit(1)

    try:
        #get info by v$instance
        connect = 1
        instance_name = oracle.get_instance(conn,'instance_name')
        instance_role = oracle.get_instance(conn,'instance_role')
        database_role = oracle.get_database(conn,'database_role')
        open_mode = oracle.get_database(conn,'open_mode')
        protection_mode = oracle.get_database(conn,'protection_mode')
        if database_role == 'PRIMARY':  
            database_role_new = 'm'  
            dg_stats = '-1'
            dg_delay = '-1'
        else:  
            database_role_new = 's'
            dg_stats = oracle.get_dg_stats(conn)
            dg_delay = oracle.get_dg_delay(conn)
        instance_status = oracle.get_instance(conn,'status')
        startup_time = oracle.get_instance(conn,'startup_time')
        #print startup_time
        #startup_time = time.strftime('%Y-%m-%d %H:%M:%S',startup_time) 
        #localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        #uptime =  (localtime - startup_time).seconds        
        #print uptime
        uptime = oracle.get_instance(conn,'startup_time')
        version = oracle.get_instance(conn,'version')
        instance_status = oracle.get_instance(conn,'status')
        database_status = oracle.get_instance(conn,'database_status')
        host_name = oracle.get_instance(conn,'host_name')
        archiver = oracle.get_instance(conn,'archiver')
        #get info by sql count
        session_total = oracle.get_sessions(conn)
        session_actives = oracle.get_actives(conn)
        session_waits = oracle.get_waits(conn)
        #get info by v$parameters
        parameters = oracle.get_parameters(conn)
        processes = parameters['processes']
        
        ##get info by v$parameters
        sysstat_0 = oracle.get_sysstat(conn)
        time.sleep(1)
        sysstat_1 = oracle.get_sysstat(conn)
        session_logical_reads_persecond = sysstat_1['session logical reads']-sysstat_0['session logical reads']
        physical_reads_persecond = sysstat_1['physical reads']-sysstat_0['physical reads']
        physical_writes_persecond = sysstat_1['physical writes']-sysstat_0['physical writes']
        physical_read_io_requests_persecond = sysstat_1['physical write total IO requests']-sysstat_0['physical write total IO requests']
        physical_write_io_requests_persecond = sysstat_1['physical read IO requests']-sysstat_0['physical read IO requests']
        db_block_changes_persecond = sysstat_1['db block changes']-sysstat_0['db block changes']
        os_cpu_wait_time = sysstat_0['OS CPU Qt wait time']
        logons_persecond = sysstat_1['logons cumulative']-sysstat_0['logons cumulative']
        logons_current = sysstat_0['logons current']
        opened_cursors_persecond = sysstat_1['opened cursors cumulative']-sysstat_0['opened cursors cumulative']
        opened_cursors_current = sysstat_0['opened cursors current']
        user_commits_persecond = sysstat_1['user commits']-sysstat_0['user commits']
        user_rollbacks_persecond = sysstat_1['user rollbacks']-sysstat_0['user rollbacks']
        user_calls_persecond = sysstat_1['user calls']-sysstat_0['user calls']
        db_block_gets_persecond = sysstat_1['db block gets']-sysstat_0['db block gets']
        #print session_logical_reads_persecond

        ##################### insert data to mysql server#############################
	func.mysql_exec("replace into oracle_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from oracle_status where host='%s' and port=%s;" % (host, port),'')
	func.mysql_exec("delete from oracle_status where host='%s' and port=%s;" % (host, port),'')

        sql = "insert into oracle_status(server_id,host,port,tags,connect,instance_name,instance_role,instance_status,database_role,open_mode,protection_mode,host_name,database_status,startup_time,uptime,version,archiver,session_total,session_actives,session_waits,dg_stats,dg_delay,processes,session_logical_reads_persecond,physical_reads_persecond,physical_writes_persecond,physical_read_io_requests_persecond,physical_write_io_requests_persecond,db_block_changes_persecond,os_cpu_wait_time,logons_persecond,logons_current,opened_cursors_persecond,opened_cursors_current,user_commits_persecond,user_rollbacks_persecond,user_calls_persecond,db_block_gets_persecond) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        param = (server_id,host,port,tags,connect,instance_name,instance_role,instance_status,database_role,open_mode,protection_mode,host_name,database_status,startup_time,uptime,version,archiver,session_total,session_actives,session_waits,dg_stats,dg_delay,processes,session_logical_reads_persecond,physical_reads_persecond,physical_writes_persecond,physical_read_io_requests_persecond,physical_write_io_requests_persecond,db_block_changes_persecond,os_cpu_wait_time,logons_persecond,logons_current,opened_cursors_persecond,opened_cursors_current,user_commits_persecond,user_rollbacks_persecond,user_calls_persecond,db_block_gets_persecond)
        func.mysql_exec(sql,param) 
        logger.info("Finish INSERT DATA ")
        func.update_db_status_init(server_id,database_role_new,version,host,port,tags)
        logger.info("Finish update_db_status_init")

        #check tablespace
	qSql = "select 1 from oracle_tablespace where host='%s' and port=%s and create_time>=curdate() limit 1" % (host,port)
	a = func.mysql_query(qSql)
	if func.mysql_query(qSql) == 0:
	    func.mysql_exec("insert ignore into oracle_tablespace_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from oracle_tablespace where host='%s' and port=%s;" % (host, port),'')
	    func.mysql_exec("delete from oracle_tablespace where host='%s' and port=%s;" % (host, port),'')
	    tablespace = oracle.get_tablespace(conn)
	    if tablespace:
		for line in tablespace:
		    ts_name=line[0]
		    if igTsNames.count(ts_name) > 0:
			continue
		    sql="insert into oracle_tablespace(server_id,host,port,tags,tablespace_name,total_size,used_size,avail_size,used_rate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		    param=(server_id,host,port,tags,line[0],line[1],line[2],line[3],int(line[4].rstrip("%")))
		    logger.info(param)
		    func.mysql_exec(sql,param)
        else:
	    logger.info("%s:%s today has stat oracle_tablespace. will not do" % (host,port))
        logger.info("Finish oracle_tablespace")

    except Exception, e:
        logger.error(e)
        sys.exit(1)

    finally:
        conn.close()
        




def main():
    #get oracle servers list
    #servers=func.mysql_query("select id,host,port,dsn,username,password,tags from db_servers_oracle where is_delete=0 and monitor=1;")
    servers=func.mysql_query("select id,host,port,dsn,tags from db_servers_oracle where is_delete=0 and monitor=1;")


    #++ guoqi
    cnfKey = "monitor_oracle"
    username = func.get_config(cnfKey,'user')
    password = func.get_config(cnfKey,'passwd')
    min_interval = func.get_option('min_interval')

    logger.info("check oracle controller start.")
    if servers:
        plist = []
        for row in servers:
	    (server_id, host, port, dsn, tags) = row
            p = Process(target = check_oracle, args = (host,port,dsn,username,password,server_id,tags))
            plist.append(p)
            p.start()
        #time.sleep(10)
        #for p in plist:
        #    p.terminate()
        for p in plist:
            p.join()

    else:
        logger.warning("check oracle: not found any servers")

    func.mysql_exec('update oracle_status set connect=0,create_time=now()  where create_time<date_sub(now(), interval %s second)' % (min_interval))
    func.mysql_exec('DELETE ot FROM oracle_tablespace AS ot, db_servers_oracle AS d where (d.is_delete=1 or d.monitor=0) AND ot.host=d.host AND ot.port=d.port')
    func.mysql_exec('DELETE ot FROM oracle_status AS ot, db_servers_oracle AS d where (d.is_delete=1 or d.monitor=0) AND ot.host=d.host AND ot.port=d.port')
    #func.mysql_exec('DELETE ds FROM oracle_status AS ds, (SELECT s.id,d.host FROM oracle_status AS s LEFT JOIN db_servers_oracle AS d  ON d.is_delete=0 AND d.monitor=1 AND s.host=d.host AND s.port=d.port  HAVING d.`host` IS NULL) AS t WHERE ds.id=t.id')
    func.mysql_exec('DELETE ds FROM db_status AS ds, (SELECT s.id,d.host FROM db_status AS s LEFT JOIN db_servers_oracle AS d  ON d.is_delete=0 AND d.monitor=1 AND s.host=d.host AND s.port=d.port  WHERE db_type="oracle"  HAVING d.`host` IS NULL) AS t WHERE ds.id=t.id')

    logger.info("check oracle controller finished.")
                     


if __name__=='__main__':
    igTsNames = ["SYSAUX", "SYSTEM"]
    main()
