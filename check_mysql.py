#!/usr/bin/env python
#coding:utf-8
import os
import sys
import string
import time
import datetime
import re
import MySQLdb
import argparse
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("mysql")
path='./include'
sys.path.insert(0,path)
import functions as func
import camelbell_mysql as mysql
from multiprocessing import Process;


def check_mysql(host,port,username,password,server_id,tags):
    logger_msg = "[BBQ]begin check mysql %s:%s " %(host,port)
    logger.info(logger_msg)
    try:
        conn=MySQLdb.connect(host=host,user=username,passwd=password,port=int(port),connect_timeout=3,charset='utf8')
        cur=conn.cursor()
        conn.select_db('information_schema')
        #cur.execute('flush hosts;')
	saveMysqlStatus = {}
        saveMysqlStatus['host'] = host
        saveMysqlStatus['port'] = port
        saveMysqlStatus['server_id'] = server_id
        saveMysqlStatus['tags'] = tags
        saveMysqlStatus['connect'] = 1
        ############################# CHECK MYSQL ####################################################
        mysql_variables = func.get_mysql_variables(cur)
        mysql_status = func.get_mysql_status(cur)
        logger_msg = "[BBQ]get mysql %s:%s status1 " %(host,port)
        logger.info(logger_msg)
        time.sleep(1)
        mysql_status_2 = func.get_mysql_status(cur)
        logger_msg = "[BBQ]get mysql %s:%s status2 " %(host,port)
        logger.info(logger_msg)
        ############################# GET VARIABLES ###################################################
        version = func.get_item(mysql_variables,'version')
        saveMysqlStatus['version'] = version
        saveMysqlStatus['innodb_stats_on_metadata'] = func.get_item(mysql_variables,'innodb_stats_on_metadata')
        saveMysqlStatus['sync_binlog'] = func.get_item(mysql_variables,'sync_binlog')
        saveMysqlStatus['key_buffer_size'] = func.get_item(mysql_variables,'key_buffer_size')
        saveMysqlStatus['sort_buffer_size'] = func.get_item(mysql_variables,'sort_buffer_size')
        saveMysqlStatus['join_buffer_size'] = func.get_item(mysql_variables,'join_buffer_size')
        saveMysqlStatus['max_connections'] = func.get_item(mysql_variables,'max_connections')
        saveMysqlStatus['max_connect_errors'] = func.get_item(mysql_variables,'max_connect_errors')
        saveMysqlStatus['open_files_limit'] = func.get_item(mysql_variables,'open_files_limit')
        saveMysqlStatus['table_open_cache'] = func.get_item(mysql_variables,'table_open_cache')
        saveMysqlStatus['max_tmp_tables'] = func.get_item(mysql_variables,'max_tmp_tables')
        saveMysqlStatus['max_heap_table_size'] = func.get_item(mysql_variables,'max_heap_table_size')
        saveMysqlStatus['max_allowed_packet'] = func.get_item(mysql_variables,'max_allowed_packet')
        
        ############################# GET INNODB INFO ##################################################
        #innodb variables
        saveMysqlStatus['innodb_version'] = func.get_item(mysql_variables,'innodb_version')
        saveMysqlStatus['innodb_buffer_pool_instances'] = func.get_item(mysql_variables,'innodb_buffer_pool_instances')
        saveMysqlStatus['innodb_buffer_pool_size'] = func.get_item(mysql_variables,'innodb_buffer_pool_size')
        saveMysqlStatus['innodb_doublewrite'] = func.get_item(mysql_variables,'innodb_doublewrite')
        saveMysqlStatus['innodb_file_per_table'] = func.get_item(mysql_variables,'innodb_file_per_table')
        saveMysqlStatus['innodb_flush_log_at_trx_commit'] = func.get_item(mysql_variables,'innodb_flush_log_at_trx_commit')
        saveMysqlStatus['innodb_flush_method'] = func.get_item(mysql_variables,'innodb_flush_method')
        saveMysqlStatus['innodb_force_recovery'] = func.get_item(mysql_variables,'innodb_force_recovery')
        saveMysqlStatus['innodb_io_capacity'] = func.get_item(mysql_variables,'innodb_io_capacity')
        saveMysqlStatus['innodb_read_io_threads'] = func.get_item(mysql_variables,'innodb_read_io_threads')
        saveMysqlStatus['innodb_write_io_threads'] = func.get_item(mysql_variables,'innodb_write_io_threads')
        #innodb status
        saveMysqlStatus['innodb_buffer_pool_pages_total'] = int(func.get_item(mysql_status,'Innodb_buffer_pool_pages_total'))
        saveMysqlStatus['innodb_buffer_pool_pages_data'] = int(func.get_item(mysql_status,'Innodb_buffer_pool_pages_data'))
        saveMysqlStatus['innodb_buffer_pool_pages_dirty'] = int(func.get_item(mysql_status,'Innodb_buffer_pool_pages_dirty'))
        saveMysqlStatus['innodb_buffer_pool_pages_flushed'] = int(func.get_item(mysql_status,'Innodb_buffer_pool_pages_flushed'))
        saveMysqlStatus['innodb_buffer_pool_pages_free'] = int(func.get_item(mysql_status,'Innodb_buffer_pool_pages_free'))
        saveMysqlStatus['innodb_buffer_pool_pages_misc'] = int(func.get_item(mysql_status,'Innodb_buffer_pool_pages_misc'))
        saveMysqlStatus['innodb_page_size'] = int(func.get_item(mysql_status,'Innodb_page_size'))
        saveMysqlStatus['innodb_pages_created'] = int(func.get_item(mysql_status,'Innodb_pages_created'))
        saveMysqlStatus['innodb_pages_read'] = int(func.get_item(mysql_status,'Innodb_pages_read'))
        saveMysqlStatus['innodb_pages_written'] = int(func.get_item(mysql_status,'Innodb_pages_written'))
        saveMysqlStatus['innodb_row_lock_current_waits'] = int(func.get_item(mysql_status,'Innodb_row_lock_current_waits'))
        #innodb persecond info
        saveMysqlStatus['innodb_buffer_pool_read_requests_persecond'] = int(func.get_item(mysql_status_2,'Innodb_buffer_pool_read_requests')) - int(func.get_item(mysql_status,'Innodb_buffer_pool_read_requests'))
        saveMysqlStatus['innodb_buffer_pool_reads_persecond'] = int(func.get_item(mysql_status_2,'Innodb_buffer_pool_reads')) - int(func.get_item(mysql_status,'Innodb_buffer_pool_reads'))
        saveMysqlStatus['innodb_buffer_pool_write_requests_persecond'] = int(func.get_item(mysql_status_2,'Innodb_buffer_pool_write_requests')) - int(func.get_item(mysql_status,'Innodb_buffer_pool_write_requests'))
        saveMysqlStatus['innodb_buffer_pool_pages_flushed_persecond'] = int(func.get_item(mysql_status_2,'Innodb_buffer_pool_pages_flushed')) - int(func.get_item(mysql_status,'Innodb_buffer_pool_pages_flushed'))
        saveMysqlStatus['innodb_rows_deleted_persecond'] = int(func.get_item(mysql_status_2,'Innodb_rows_deleted')) - int(func.get_item(mysql_status,'Innodb_rows_deleted'))
        saveMysqlStatus['innodb_rows_inserted_persecond'] = int(func.get_item(mysql_status_2,'Innodb_rows_inserted')) - int(func.get_item(mysql_status,'Innodb_rows_inserted'))
        saveMysqlStatus['innodb_rows_read_persecond'] = int(func.get_item(mysql_status_2,'Innodb_rows_read')) - int(func.get_item(mysql_status,'Innodb_rows_read'))
        saveMysqlStatus['innodb_rows_updated_persecond'] = int(func.get_item(mysql_status_2,'Innodb_rows_updated')) - int(func.get_item(mysql_status,'Innodb_rows_updated'))
        ############################# GET STATUS ##################################################
        saveMysqlStatus['uptime'] = func.get_item(mysql_status,'Uptime')
        saveMysqlStatus['open_files'] = func.get_item(mysql_status,'Open_files')
        saveMysqlStatus['open_tables'] = func.get_item(mysql_status,'Open_tables')
        saveMysqlStatus['threads_connected'] = func.get_item(mysql_status,'Threads_connected')
        saveMysqlStatus['threads_running'] = func.get_item(mysql_status,'Threads_running')
        #saveMysqlStatus['threads_created'] = func.get_item(mysql_status,'Threads_created')
        saveMysqlStatus['threads_created'] = int(func.get_item(mysql_status_2,'Threads_created')) - int(func.get_item(mysql_status,'Threads_created'))
        saveMysqlStatus['threads_cached'] = func.get_item(mysql_status,'Threads_cached')
        saveMysqlStatus['threads_waits'] = mysql.get_waits(conn)
        saveMysqlStatus['connections'] = func.get_item(mysql_status,'Connections')
        saveMysqlStatus['aborted_clients'] = func.get_item(mysql_status,'Aborted_clients')
        saveMysqlStatus['aborted_connects'] = func.get_item(mysql_status,'Aborted_connects')
        saveMysqlStatus['key_blocks_not_flushed'] = func.get_item(mysql_status,'Key_blocks_not_flushed')
        saveMysqlStatus['key_blocks_unused'] = func.get_item(mysql_status,'Key_blocks_unused')
        saveMysqlStatus['key_blocks_used'] = func.get_item(mysql_status,'Key_blocks_used')
        ############################# GET STATUS PERSECOND ##################################################
        saveMysqlStatus['connections_persecond'] = int(func.get_item(mysql_status_2,'Connections')) - int(func.get_item(mysql_status,'Connections'))
        saveMysqlStatus['bytes_received_persecond'] = (int(func.get_item(mysql_status_2,'Bytes_received')) - int(func.get_item(mysql_status,'Bytes_received')))/1024
        saveMysqlStatus['bytes_sent_persecond'] = (int(func.get_item(mysql_status_2,'Bytes_sent')) - int(func.get_item(mysql_status,'Bytes_sent')))/1024
        saveMysqlStatus['com_select_persecond'] = int(func.get_item(mysql_status_2,'Com_select')) - int(func.get_item(mysql_status,'Com_select'))
        saveMysqlStatus['com_insert_persecond'] = int(func.get_item(mysql_status_2,'Com_insert')) - int(func.get_item(mysql_status,'Com_insert'))
        saveMysqlStatus['com_update_persecond'] = int(func.get_item(mysql_status_2,'Com_update')) - int(func.get_item(mysql_status,'Com_update'))
        saveMysqlStatus['com_delete_persecond'] = int(func.get_item(mysql_status_2,'Com_delete')) - int(func.get_item(mysql_status,'Com_delete'))
        saveMysqlStatus['com_commit_persecond'] = int(func.get_item(mysql_status_2,'Com_commit')) - int(func.get_item(mysql_status,'Com_commit'))
        saveMysqlStatus['com_rollback_persecond'] = int(func.get_item(mysql_status_2,'Com_rollback')) - int(func.get_item(mysql_status,'Com_rollback'))
        saveMysqlStatus['questions_persecond'] = int(func.get_item(mysql_status_2,'Questions')) - int(func.get_item(mysql_status,'Questions'))
        saveMysqlStatus['queries_persecond'] = int(func.get_item(mysql_status_2,'Queries')) - int(func.get_item(mysql_status,'Queries'))
        saveMysqlStatus['transaction_persecond'] = (int(func.get_item(mysql_status_2,'Com_commit')) + int(func.get_item(mysql_status_2,'Com_rollback'))) - (int(func.get_item(mysql_status,'Com_commit')) + int(func.get_item(mysql_status,'Com_rollback')))
        saveMysqlStatus['created_tmp_disk_tables_persecond'] = int(func.get_item(mysql_status_2,'Created_tmp_disk_tables')) - int(func.get_item(mysql_status,'Created_tmp_disk_tables'))
        saveMysqlStatus['created_tmp_files_persecond'] = int(func.get_item(mysql_status_2,'Created_tmp_files')) - int(func.get_item(mysql_status,'Created_tmp_files'))
        saveMysqlStatus['created_tmp_tables_persecond'] = int(func.get_item(mysql_status_2,'Created_tmp_tables')) - int(func.get_item(mysql_status,'Created_tmp_tables'))
        saveMysqlStatus['table_locks_immediate_persecond'] = int(func.get_item(mysql_status_2,'Table_locks_immediate')) - int(func.get_item(mysql_status,'Table_locks_immediate'))
        saveMysqlStatus['table_locks_waited_persecond'] = int(func.get_item(mysql_status_2,'Table_locks_waited')) - int(func.get_item(mysql_status,'Table_locks_waited'))
        saveMysqlStatus['key_read_requests_persecond'] = int(func.get_item(mysql_status_2,'Key_read_requests')) - int(func.get_item(mysql_status,'Key_read_requests'))
        saveMysqlStatus['key_reads_persecond'] = int(func.get_item(mysql_status_2,'Key_reads')) - int(func.get_item(mysql_status,'Key_reads'))
        saveMysqlStatus['key_write_requests_persecond'] = int(func.get_item(mysql_status_2,'Key_write_requests')) - int(func.get_item(mysql_status,'Key_write_requests'))
        saveMysqlStatus['key_writes_persecond'] = int(func.get_item(mysql_status_2,'Key_writes')) - int(func.get_item(mysql_status,'Key_writes'))
        ############################# GET MYSQL HITRATE ##################################################
        if (string.atof(func.get_item(mysql_status,'Qcache_hits')) + string.atof(func.get_item(mysql_status,'Com_select'))) <> 0:
            query_cache_hitrate = string.atof(func.get_item(mysql_status,'Qcache_hits')) / (string.atof(func.get_item(mysql_status,'Qcache_hits')) + string.atof(func.get_item(mysql_status,'Com_select')))
            query_cache_hitrate =  "%9.2f" %query_cache_hitrate
        else:
            query_cache_hitrate = 0
	saveMysqlStatus['query_cache_hitrate'] = query_cache_hitrate

        if string.atof(func.get_item(mysql_status,'Connections')) <> 0:
            thread_cache_hitrate = 1 - string.atof(func.get_item(mysql_status,'Threads_created')) / string.atof(func.get_item(mysql_status,'Connections'))
            thread_cache_hitrate =  "%9.2f" %thread_cache_hitrate
        else:
            thread_cache_hitrate = 0
	saveMysqlStatus['thread_cache_hitrate'] = thread_cache_hitrate

        if string.atof(func.get_item(mysql_status,'Key_read_requests')) <> 0:
            key_buffer_read_rate = 1 - string.atof(func.get_item(mysql_status,'Key_reads')) / string.atof(func.get_item(mysql_status,'Key_read_requests'))
            key_buffer_read_rate =  "%9.2f" %key_buffer_read_rate
        else:
            key_buffer_read_rate = 0
	saveMysqlStatus['key_buffer_read_rate'] = key_buffer_read_rate

        if string.atof(func.get_item(mysql_status,'Key_write_requests')) <> 0:
            key_buffer_write_rate = 1 - string.atof(func.get_item(mysql_status,'Key_writes')) / string.atof(func.get_item(mysql_status,'Key_write_requests'))
            key_buffer_write_rate =  "%9.2f" %key_buffer_write_rate
        else:
            key_buffer_write_rate = 0
	saveMysqlStatus['key_buffer_write_rate'] = key_buffer_write_rate

        if (string.atof(func.get_item(mysql_status,'Key_blocks_used'))+string.atof(func.get_item(mysql_status,'Key_blocks_unused'))) <> 0:
            key_blocks_used_rate = string.atof(func.get_item(mysql_status,'Key_blocks_used')) / (string.atof(func.get_item(mysql_status,'Key_blocks_used'))+string.atof(func.get_item(mysql_status,'Key_blocks_unused')))
            key_blocks_used_rate =  "%9.2f" %key_blocks_used_rate
        else:
            key_blocks_used_rate = 0
	saveMysqlStatus['key_blocks_used_rate'] = key_blocks_used_rate

        if (string.atof(func.get_item(mysql_status,'Created_tmp_disk_tables'))+string.atof(func.get_item(mysql_status,'Created_tmp_tables'))) <> 0:
            created_tmp_disk_tables_rate = string.atof(func.get_item(mysql_status,'Created_tmp_disk_tables')) / (string.atof(func.get_item(mysql_status,'Created_tmp_disk_tables'))+string.atof(func.get_item(mysql_status,'Created_tmp_tables')))
            created_tmp_disk_tables_rate =  "%9.2f" %created_tmp_disk_tables_rate
        else:
            created_tmp_disk_tables_rate = 0
	saveMysqlStatus['created_tmp_disk_tables_rate'] = created_tmp_disk_tables_rate

	max_connections = saveMysqlStatus.get('max_connections')
	threads_connected = saveMysqlStatus.get('threads_connected')
        if string.atof(max_connections) <> 0:
            connections_usage_rate = string.atof(threads_connected)/string.atof(max_connections)
            connections_usage_rate =  "%9.2f" %connections_usage_rate
        else:
            connections_usage_rate = 0
	saveMysqlStatus['connections_usage_rate'] = connections_usage_rate

	open_files_limit = saveMysqlStatus.get('open_files_limit')
	open_files = saveMysqlStatus.get('open_files')
        if string.atof(open_files_limit) <> 0:            
            open_files_usage_rate = string.atof(open_files)/string.atof(open_files_limit)
            open_files_usage_rate =  "%9.2f" %open_files_usage_rate
        else:
            open_files_usage_rate = 0
	saveMysqlStatus['open_files_usage_rate'] = open_files_usage_rate

	table_open_cache = saveMysqlStatus.get('table_open_cache')
	open_tables = saveMysqlStatus.get('open_tables')
        if string.atof(table_open_cache) <> 0:            
            open_tables_usage_rate = string.atof(open_tables)/string.atof(table_open_cache)
            open_tables_usage_rate =  "%9.2f" %open_tables_usage_rate
        else:
            open_tables_usage_rate = 0
	saveMysqlStatus['open_tables_usage_rate'] = open_tables_usage_rate
  
        #repl
	read_only = func.get_item(mysql_variables,'read_only')
        slave_hosts=cur.execute('show slave hosts;')
        slave_status=cur.execute('show slave status;')
        if (slave_status == 0) or (slave_status <> 0 and slave_hosts>0 and (not cmp(read_only, "OFF"))):
            role='master'
            role_new='m'
        else:
            role='slave'
            role_new='s'
	saveMysqlStatus['role'] = role

        ############################# disk size          ##################################################
	disk_size_m = 0
	saveMysqlStatus['disk_size_m'] = disk_size_m

        ############################# INSERT INTO SERVER ##################################################
        updSqls = []
	updSqls.append("replace into mysql_status_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from mysql_status where host='%s' and port=%s" % (host,port))
    	updSqls.append("delete from mysql_status where host='%s' and port=%s" % (host,port))
	cols = []
	vals = []
	for key, val in saveMysqlStatus.iteritems():
	    cols.append(key)
	    vals.append(str(val))
	insSql = "insert into mysql_status(%s) VALUES ('%s')" % (",".join(cols), "','".join(vals))
    	updSqls.append(insSql)
        #logger.info(updSqls)
        func.mysql_exec_many(updSqls)
        logger_msg = "[BBQ]save mysql %s:%s status " %(host,port)
        logger.info(logger_msg)
        func.update_db_status_init(server_id,role_new,version,host,port,tags)

	# save other
	func.other_save("mysql_status", saveMysqlStatus)

        #check mysql process
        processlist=cur.execute('select * from information_schema.processlist where DB !="information_schema" and command !="Sleep";')
        if processlist:
            for line in cur.fetchall():
                sql="insert into mysql_processlist(server_id,host,port,tags,pid,p_user,p_host,p_db,command,time,status,info) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                param=(server_id,host,port,tags,line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7])
                func.mysql_exec(sql,param)

        #check mysql connected
        connected=cur.execute("select SUBSTRING_INDEX(host,':',1) as connect_server, user connect_user,db connect_db, count(SUBSTRING_INDEX(host,':',1)) as connect_count  from information_schema.processlist where db is not null and db!='information_schema' and db !='performance_schema' group by connect_server, connect_user, connect_db ;");
        if connected:
            for line in cur.fetchall():
                sql="insert into mysql_connected(server_id,host,port,tags,connect_server,connect_user,connect_db,connect_count) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                param =(server_id,host,port,tags,line[0],line[1],line[2],line[3])
                func.mysql_exec(sql,param)

        #check mysql replication
        master_thread=cur.execute("select * from information_schema.processlist where COMMAND = 'Binlog Dump' or COMMAND = 'Binlog Dump GTID';")
        slave_status=cur.execute('show slave status;')
        datalist=[]
        if master_thread >= 1:
            datalist.append(int(1))
            if not cmp("slave",role):
                datalist.append(int(1))
            else:
                datalist.append(int(0))
        else:
            datalist.append(int(0))
            if not cmp("slave",role):
                datalist.append(int(1))
            else:
                datalist.append(int(0))

	gtid_mode=cur.execute("select * from information_schema.global_variables where variable_name='gtid_mode';")
	result=cur.fetchone()
	if result:
	    gtid_mode=result[1]
	else:
	    gtid_mode='OFF'
	datalist.append(gtid_mode)

	read_only=cur.execute("show variables where variable_name like 'read_only';")
	result=cur.fetchone()
	datalist.append(result[1])

	master_binlog_file='---'
	master_binlog_pos='---'
	master_binlog_space = 0
	slave_info=cur.execute('show slave status;')
	if slave_info == 0:
	    for i in range(0,7):
		datalist.append('-1')
	else:	
	    result=cur.fetchone()
	    master_server=result[1]
	    master_port=result[3]
	    master_binlog_file=result[5]
	    master_binlog_pos=result[6]
	    current_binlog_file=result[9]
	    slave_io_run=result[10]
	    slave_sql_run=result[11]
	    current_binlog_pos=result[21]
	    delay=result[32]
	    # delay use hearbeat
	    masterHosts = func.mysql_query("select host from db_servers_mysql where host='%s' or replicate_ip='%s' limit 1" % (master_server,master_server))
	    if masterHosts != 0:
		masterHost =  masterHosts[0][0]
		connMaster = MySQLdb.connect(host=masterHost,user=username,passwd=password,port=int(master_port),connect_timeout=3,charset='utf8')
		curMaster = connMaster.cursor()
		connMaster.select_db('information_schema')
		master_variables = func.get_mysql_variables(curMaster)
		master_server_id = master_variables.get("server_id")
		connMaster.close()
		# query delay by master_server_id
		qHb = "select UNIX_TIMESTAMP()-UNIX_TIMESTAMP(STR_TO_DATE(substring_index(ts,'.',1),'%%Y-%%m-%%dT%%H:%%i:%%s')) as d from dhdba.heartbeat where server_id='%s';" % (master_server_id)
		cur.execute(qHb)
		lines = cur.fetchone()
		if lines:
		    delay = int(lines[0])
		    if delay < 0:
			delay = 0
	    datalist.append(master_server)
	    datalist.append(master_port)
	    datalist.append(slave_io_run)
	    datalist.append(slave_sql_run)
	    datalist.append(delay)
	    datalist.append(current_binlog_file)
	    datalist.append(current_binlog_pos)
	master=cur.execute('show master status;')
	if master != 0:
	    master_result=cur.fetchone()
	    master_binlog_file=master_result[0]
	    master_binlog_pos=master_result[1]
	binlog_file=cur.execute('show master logs;')
	if binlog_file:
	    for row in cur.fetchall():
		master_binlog_space = master_binlog_space + int(row[1])

	datalist.append(master_binlog_file)
	datalist.append(master_binlog_pos)
	datalist.append(master_binlog_space)

        result=datalist
        if result:
	    func.mysql_exec("replace into mysql_replication_history SELECT *,LEFT(REPLACE(REPLACE(REPLACE(create_time,'-',''),' ',''),':',''),12) from mysql_replication where host='%s' and port=%s" % (host, port),'')
	    func.mysql_exec("delete from mysql_replication where host='%s' and port=%s" % (host, port),'')
            sql="insert into mysql_replication(server_id,tags,host,port,is_master,is_slave,gtid_mode,read_only,master_server,master_port,slave_io_run,slave_sql_run,delay,current_binlog_file,current_binlog_pos,master_binlog_file,master_binlog_pos,master_binlog_space) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            param=(server_id,tags,host,port,result[0],result[1],result[2],result[3],result[4],result[5],result[6],result[7],result[8],result[9],result[10],result[11],result[12],result[13])
            func.mysql_exec(sql,param)

        cur.close()
    except MySQLdb.Error,e:
        logger_msg="check mysql %s:%s failure: %d %s" %(host,port,e.args[0],e.args[1])
        logger.warning(logger_msg)
        logger_msg="check mysql %s:%s failure: sleep 3 seconds and check again." %(host,port)
        logger.warning(logger_msg)
        time.sleep(3)
        try:
            conn=MySQLdb.connect(host=host,user=username,passwd=password,port=int(port),connect_timeout=3,charset='utf8')
            cur=conn.cursor()
            conn.select_db('information_schema')
        except MySQLdb.Error,e:
            logger_msg="check mysql second %s:%s failure: %d %s" %(host,port,e.args[0],e.args[1])
            logger.warning(logger_msg)
            connect = 0
            sql="replace into mysql_status(server_id,host,port,tags,connect) values(%s,%s,%s,%s,%s)"
            param=(server_id,host,port,tags,connect)
            func.mysql_exec(sql,param)
   
    try:  
        func.check_db_status(server_id,host,port,tags,'mysql')   
    except Exception, e:
        logger.error(e)
        sys.exit(1)
         
   

def main(hosts=None):
    dohosts = None
    if hosts != None:
	dohosts = hosts.split(",")
    #get mysql servers list
    #servers = func.mysql_query('select id,host,port,username,password,tags from db_servers_mysql where is_delete=0 and monitor=1;')
    servers = func.mysql_query('select id,host,port,tags from db_servers_mysql where is_delete=0 and monitor=1 order by rand();')

    logger.info("check mysql controller started.")

    #++ guoqi
    exeTimeout = 60
    cnfKey = "monitor_mysql"
    username = func.get_config(cnfKey,'user')
    password = func.get_config(cnfKey,'passwd')
    min_interval = func.get_option('min_interval')

    if servers:
	plist = []
	for row in servers:
	    (server_id, host, port, tags) = row
	    if dohosts != None and dohosts.count(host)<=0:
		continue 
	    check_mysql(host,port,username,password,server_id,tags)
	    continue
	    plist = []
	    (server_id, host, port, tags) = row
	    p = Process(target = check_mysql, args = (host,port,username,password,server_id,tags))
	    p.start()

	    i = 0
	    while(i<10):
		time.sleep(1)
		i += 1
		if p.is_alive():
		    continue
	    p.join(timeout=2)

	'''
	for p in plist:
	    p.start()
	for p in plist:
	    p.join(timeout=exeTimeout)
	'''
    else:
         logger.warning("check mysql: not found any servers")

    func.mysql_exec('DELETE ds FROM mysql_replication AS ds, (SELECT s.id,d.host FROM mysql_replication AS s LEFT JOIN db_servers_mysql AS d  ON d.is_delete=0 AND d.monitor=1 AND s.host=d.host AND s.port=d.port HAVING d.`host` IS NULL) AS t  WHERE ds.id=t.id')
    func.mysql_exec('DELETE ds FROM mysql_status AS ds, (SELECT s.id,d.host FROM mysql_status AS s LEFT JOIN db_servers_mysql AS d  ON d.is_delete=0 AND d.monitor=1 AND s.host=d.host AND s.port=d.port HAVING d.`host` IS NULL) AS t  WHERE ds.id=t.id')
    func.mysql_exec('DELETE ds FROM db_status AS ds, (SELECT s.id,d.host FROM db_status AS s LEFT JOIN db_servers_mysql AS d  ON d.is_delete=0 AND d.monitor=1 AND s.host=d.host AND s.port=d.port  WHERE db_type="mysql"  HAVING d.`host` IS NULL) AS t  WHERE ds.id=t.id')
    func.mysql_exec('update mysql_status set connect=0,create_time=now()  where create_time<date_sub(now(), interval %s second)' % (min_interval))
    func.mysql_exec('update mysql_replication set slave_io_run="No",slave_sql_run="No",create_time=now()  where create_time<date_sub(now(), interval %s second)' % (min_interval))
    logger.info("check mysql controller finished.")


if __name__=='__main__':
    
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("-H","--hosts", dest="hosts", help="")
    args = parser.parse_args()
    main(args.hosts)
