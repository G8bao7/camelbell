#!/bin/env python
#-*-coding:utf-8-*-

import MySQLdb
import string
import sys 
reload(sys) 
sys.setdefaultencoding('utf8')

def getMysql(host,port,user,passwd,db='mysql'):
    try:
        conn = MySQLdb.connect(host=host,port=int(port),user=user,passwd=passwd,connect_timeout=5,charset='utf8')
        conn.select_db(db)
        return conn
    except Exception,e:
        print "Error :mysql connect %s" % (e)
    return None
    
def doMysqlQuery(conn, qSql):
    try:
        cur = conn.cursor()
        cur.execute(qSql)
        lines = cur.fetchall()
        return lines
    except MySQLdb.Error, e:
        print e
    finally:
        pass
        
    return None

