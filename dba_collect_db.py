#!/usr/bin/env python
# encoding: utf-8
import os,sys,signal,re
import argparse
from datetime import datetime
import MySQLdb
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("main")
path='./include'
sys.path.insert(0,path)
import functions as func
import mysql_client_class as mysClient

def saveTables(ip, port, dbs):
    print "save %s:%s begin" % (ip, port)
    insDbSql = "REPLACE INTO mysql_databases(db_name, db_ip, db_port) VALUES (%s, %s, %s)"
    insDbVals = []
    insTbSql = "REPLACE INTO mysql_tables(db_name, tb_name, has_primary, rows, data_len, index_count, index_len) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    insTbVals = []

    for dbName, tbs in dbs.iteritems():
	insDbVals.append([dbName, ip, port])
	for tbName, tbInfos in tbs.iteritems():
	    insTbVals.append([dbName, tbName, tbInfos.get("has_primary"), tbInfos.get("rows"), tbInfos.get("data_len"), tbInfos.get("index_count"), tbInfos.get("index_len")])

    # save
    print insDbSql
    #print insDbVals
    func.mysql_exec(insDbSql, insDbVals)
    print insTbSql
    #print insTbVals
    func.mysql_exec(insTbSql, insTbVals)

    print "save %s:%s end" % (ip, port)
   

'''
{db:{tb:{db_col_name:col_val}}}
'''
def collect_tables(myCli):
    dbs = {}
    igDbs = ['information_schema','performance_schema','mysql']
    dbSql = "show databases "
    dblines = myCli.doSelSql(dbSql)
    for dbline in dblines:
        dbName = dbline.get("Database")
        if igDbs.count(dbName) > 0:
	    continue
        tbs = {}
        tbSql = "show tables FROM %s" %(dbName)
        tblines = myCli.doSelSql(tbSql)
        for tbline in tblines:
            tbName = tbline.get("Tables_in_%s" % (dbName))
            # table status
            stSql = '''show table status FROM `%s` where name = '%s' ''' % (dbName, tbName)
            tbSt = (myCli.doSelSql(stSql))[0]
            if tbSt.get("Engine") == None:
                continue
            
            # table index
            indexSql = '''SHOW INDEX FROM `%s` FROM `%s` WHERE SEQ_IN_INDEX ='1' ;''' % (tbName, dbName)
            tbIdxs = myCli.doSelSql(indexSql)
            has_primary = 0
            for tbIdx in tbIdxs:
                if not cmp("PRIMARY", tbIdx.get("Key_name")):
                    has_primary = 1
                    break

            # db_col : value            
            tbStatus = {}
            tbStatus["rows"] = int(tbSt.get("Rows"))
            tbStatus["data_len"] = int(tbSt.get("Data_length"))
            tbStatus["index_len"] = int(tbSt.get("Index_length"))
            tbStatus["has_primary"] = has_primary
            tbStatus["index_count"] = len(tbIdxs) - has_primary
            
            tbs[tbName] = tbStatus
        dbs[dbName] = tbs
            
    return dbs

def sub_mysql(args):
    qSql = "select host, port from db_servers_mysql"
    qSql = "select host, port from mysql_status where connect=1 and role='master'"
    qSql = '''SELECT s.host, s.port,t.db_ip, t.db_port FROM mysql_status AS s 
	left join (SELECT db_ip,db_port FROM mysql_databases WHERE upd_time>DATE_SUB(CURDATE(),INTERVAL 3 DAY) GROUP BY db_ip,db_port) AS t on CONCAT_WS(',',s.host,s.`port`) = CONCAT_WS(',',t.db_ip,t.db_port) WHERE s.connect=1 AND s.role='master' having t.db_ip is null'''
    lines = func.mysql_query(qSql)

    for line in lines:
        (ip, port,sip,sport) = line
	if re.search("^172.30.", ip):
	    continue
	print datetime.now(), ip, port
        myCli = mysClient.mysqlClient(ip, port)
        if myCli == None:
            print "Error: %s, %s" % (ip, port)
        else:
            dbs = collect_tables(myCli)
	    saveTables(ip, port, dbs)
   
  
def sigint_handler(signum, frame):
    exit(1)

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")

    #
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGHUP, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

    #
    SPLITER = ","
   
    #
    parents_parser = argparse.ArgumentParser(add_help=False)

    parser = argparse.ArgumentParser(description="stat  database ")
    subparsers = parser.add_subparsers()

    # run
    mysql_parser = subparsers.add_parser('mysql', parents=[parents_parser], help='')
    mysql_parser.set_defaults(func=sub_mysql)

    args = parser.parse_args()
    args.func(args)

    exit(0)


