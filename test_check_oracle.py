#!//bin/env python
#coding:utf-8
import os
import sys
import cx_Oracle

def main():
	username="system"
	password="oracleadmin"
	url="%s:%s/%s" % ("192.168.1.175",1588,"prodb.dhgate.com")
	conn=cx_Oracle.connect(username,password,url)
	sqls = []
	#sqls.append("select name,value from v$parameter")
	sqls.append("SELECT DBID FROM v$DATABASE")
	sqls.append("SELECT INSTANCE_NUMBER FROM v$INSTANCE")
	sqls.append("SELECT MAX(SNAP_ID)-1  FROM DBA_HIST_SNAPSHOT")
	sqls.append("SELECT MAX(SNAP_ID)  FROM DBA_HIST_SNAPSHOT")
	'''
	sqls.append("Select * from table(dbms_workload_repository.awr_report_text(2174221909, 1, 36888, 36889, 0))")
	sqls.append("Select * from table(dbms_workload_repository.awr_report_html(40093083, 1, 36888, 36889, 0))")
	'''

	vals = []
	curs=conn.cursor()
	for i in range(0,4):
		sql = sqls[i]
		try:
			print sql
			curs.execute(sql.rstrip(";"))
			lines = curs.fetchall()
			vals.append(lines[0][0])
		except Exception,e:
			print e
	curs.close()

	print vals
	sql = "Select * from table(dbms_workload_repository.awr_report_text(%s,%s,%s,%s,0))" % (vals[0], vals[1], vals[2], vals[3])
	#sql = "Select * from table(dbms_workload_repository.awr_report_html(%s,%s,%s,%s,0))" % (vals[0], vals[1], vals[2], vals[3])
	print sql
	curs=conn.cursor()
	curs.execute(sql)
	lines = curs.fetchall()
	curs.close()

	f=open("/tmp/o.txt","w")
	#f=open("/tmp/o.html","w")
	for line in lines:
		f.write("%s\n" % (line))
	f.close()
if __name__=='__main__':
    main()
