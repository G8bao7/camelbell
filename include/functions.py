#!/bin/env python
#-*-coding:utf-8-*-

import MySQLdb
import string
import time  
import datetime
import os
import re
import json
import sys 
reload(sys) 
sys.setdefaultencoding('utf8')
import ConfigParser
import smtplib
from email.mime.text import MIMEText
from email.message import Message
from email.header import Header
from pymongo import MongoClient
import dba_crypto as crypt
basePath = "/usr/local/camelbell"
fCnf = "%s/etc/config.ini" % (basePath)

def get_item(data_dict,item,default='-1'):
    if data_dict.has_key(item):
	return data_dict[item]
    else:
	return default

def get_config(group,config_name,default='-1'):
    config = ConfigParser.ConfigParser()
    config.readfp(open(fCnf,'rw'))
    if config.has_option(group,config_name):
	config_value=config.get(group,config_name).strip(' ').strip('\'').strip('\"')
    else:
	config_value=default
    return config_value

def get_option(key):
    conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    conn.select_db(dbname)
    cursor = conn.cursor()
    sql="select value from options where name='%s'" % (key)
    count=cursor.execute(sql)
    if count == 0 :
        result=0
    else:
        result=(cursor.fetchone())[0]
    cursor.close()
    conn.close()
    return result

def filters(data):
    return data.strip(' ').strip('\n').strip('\br')

server_key = 'monitor_server'
host = get_config(server_key,'host')
port = get_config(server_key,'port')
user = get_config(server_key,'user')
passwd = get_config(server_key,'passwd')
dbname = get_config(server_key,'dbname')

server_mongodb_key = 'monitor_server_mongodb'
mongodb_host = get_config(server_mongodb_key,'host')
mongodb_port = get_config(server_mongodb_key,'port')
mongodb_user = get_config(server_mongodb_key,'user')
mongodb_passwd = get_config(server_mongodb_key,'passwd')
mongodb_dbname = get_config(server_mongodb_key,'dbname')
mongodb_replicaSet = get_config(server_mongodb_key,'replicaSet')

'''
# collection 函数文档
http://api.mongodb.com/python/current/api/pymongo/collection.html?_ga=1.219309790.610772200.1468379950#pymongo.collection.Collection.find
'''

def mongodb_save(tb_name, params):
   inpParams = params
   inpParams.pop("id", None)
   connect_mongodb = None
   try:
        connect_mongodb = MongoClient(host=mongodb_host,port=int(mongodb_port),replicaSet=mongodb_replicaSet)
        db = connect_mongodb.get_database(mongodb_dbname)
        db.authenticate(mongodb_user, mongodb_passwd, mechanism='SCRAM-SHA-1')
	tb = db[tb_name]
	tb.insert_one(inpParams)
	#print tb.find_one()
	#print tb.find_one(sort=[("create_time",-1)])
	print tb.count()
	'''
	tlines = tb.find(limit=10).sort([("create_time",-1)])
	for tline in tlines:
	    print tline
	'''
   except Exception, e:
        logger_msg="insert alarm to mongodb %s:%s/%s,%s/%s : %s" %(mongodb_host,mongodb_port,mongodb_dbname,mongodb_user,mongodb_passwd,e)
        print logger_msg
   finally:
	if connect_mongodb != None:
	    connect_mongodb.close()

def other_save(tb_name, params):
    mongodb_save(tb_name, params)

def mysql_exec_many(sqls,params=None):
    try:
    	conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    	conn.select_db(dbname)
    	curs = conn.cursor()
	curs.execute("set session sql_mode=''")
	for i in range(0,len(sqls)):
	    sql = sqls[i]
	    if params != None and params[i] <> '':
		curs.execute(sql,params[i])
	    else:
		curs.execute(sql)
        curs.close()
        conn.commit()
        conn.close()
    except Exception,e:
        print "mysql execute: %s,%s" % (sqls, e)
        conn.rollback() 

def mysql_exec(sql,params=''):
    try:
    	conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    	conn.select_db(dbname)
    	curs = conn.cursor()
	'''
	curs.execute("set session sql_mode=''")
	curs.execute("SELECT CONNECTION_ID() AS cid")
	cnntID = (curs.fetchall())[0][0]
	#print sql
	'''
    	if params <> '':
            if len(params) > 0 and (isinstance(params[0], tuple) or isinstance(params[0], list)):
                curs.executemany(sql, params)
	    else:
		curs.execute(sql,params)
        else:
            curs.execute(sql)
        curs.close()
        conn.commit()
        conn.close()
    except Exception, e:
        print "Error: %s" % (sql)
        print "Exception: %s" % (e)
	conn.rollback()



def mysql_query(sql):
    conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    conn.select_db(dbname)
    cursor = conn.cursor()
    #cursor.execute("set session sql_mode=''")
    count=cursor.execute(sql)
    if count == 0 :
        result=0
    else:
        result=cursor.fetchall()
    cursor.close()
    conn.close()
    return result


send_mail_max_count = get_option('send_mail_max_count')
send_mail_sleep_time = get_option('send_mail_sleep_time')
mail_to_list_common = get_option('send_mail_to_list')

def add_alarm(server_id,tags,db_host,db_port,create_time,db_type,alarm_item,alarm_value,level,message,send_mail=1,send_mail_to_list=mail_to_list_common, send_sms=0, send_sms_to_list=''):
    inpParams = locals()
    try: 
	conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
	conn.select_db(dbname)
	curs = conn.cursor()
	sql="insert into alarm(server_id,tags,host,port,create_time,db_type,alarm_item,alarm_value,level,message,send_mail,send_mail_to_list,send_sms,send_sms_to_list) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
	param=(server_id,tags,db_host,db_port,create_time,db_type,alarm_item,alarm_value,level,message,send_mail,send_mail_to_list,send_sms,send_sms_to_list)
	curs.execute(sql,param)

	if send_mail == 1:
	    temp_sql = "insert into alarm_temp(server_id,ip,db_type,alarm_item,alarm_type) values(%s,%s,%s,%s,%s);"
	    temp_param = (server_id,db_host,db_type,alarm_item,'mail')
	    curs.execute(temp_sql,temp_param)
	if send_sms == 1:
	    temp_sql = "insert into alarm_temp(server_id,ip,db_type,alarm_item,alarm_type) values(%s,%s,%s,%s,%s);"
	    temp_param = (server_id,db_host,db_type,alarm_item,'sms')
	    curs.execute(temp_sql,temp_param)
	if (send_mail ==0 and send_sms==0):
	    temp_sql = "insert into alarm_temp(server_id,ip,db_type,alarm_item,alarm_type) values(%s,%s,%s,%s,%s);"
	    temp_param = (server_id,db_host,db_type,alarm_item,'none')
	    curs.execute(temp_sql,temp_param)
	conn.commit()
	curs.close()
	conn.close()
    except Exception,e:
	print "Add alarm: " + str(e)     

       
    # insert mongodb
    mongodb_save("alarm_logs",inpParams)


def check_if_ok(server_id,tags,db_host,db_port,create_time,db_type,alarm_item,alarm_value,message,send_mail,send_mail_to_list,send_sms,send_sms_to_list):
    conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    conn.select_db(dbname)
    curs = conn.cursor()
    if db_type=='os':
        alarm_count=curs.execute("select id from alarm_temp where ip='%s' and alarm_item='%s' ;" %(db_host,alarm_item))
        mysql_exec("delete from alarm_temp where ip='%s'  and alarm_item='%s' ;" %(db_host,alarm_item),'')
    else:
        alarm_count=curs.execute("select id from alarm_temp where server_id=%s and db_type='%s' and alarm_item='%s' ;" %(server_id,db_type,alarm_item))                    
        mysql_exec("delete from alarm_temp where server_id=%s and db_type='%s' and alarm_item='%s' ;" %(server_id,db_type,alarm_item),'')

    if int(alarm_count) > 0 :
        sql="insert into alarm(server_id,tags,host,port,create_time,db_type,alarm_item,alarm_value,level,message,send_mail,send_mail_to_list,send_sms,send_sms_to_list) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        param=(server_id,tags,db_host,db_port,create_time,db_type,alarm_item,alarm_value,'ok',message,send_mail,send_mail_to_list,send_sms,send_sms_to_list)
        mysql_exec(sql,param)

    curs.close()
    conn.close()
    
    
def update_send_mail_status(server,db_type,alarm_item,send_mail,send_mail_max_count):
    conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    conn.select_db(dbname)
    curs = conn.cursor()
    if db_type == "os":
        alarm_count=curs.execute("select id from alarm_temp where ip='%s' and db_type='%s' and alarm_item='%s' and alarm_type='mail' ;" %(server,db_type,alarm_item))
    else:
        alarm_count=curs.execute("select id from alarm_temp where server_id=%s and db_type='%s' and alarm_item='%s' and alarm_type='mail' ;" %(server,db_type,alarm_item)) 
    if int(alarm_count) >= int(send_mail_max_count) :
        send_mail = 0
    else:
        send_mail = send_mail
    curs.close()
    conn.close()
    return send_mail

def update_send_sms_status(server,db_type,alarm_item,send_sms,send_sms_max_count):
    conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    conn.select_db(dbname)
    curs = conn.cursor()
    if db_type == "os":
        alarm_count=curs.execute("select id from alarm_temp where ip='%s' and db_type='%s' and alarm_item='%s' and alarm_type='sms' ;" %(server,db_type,alarm_item))
    else:
        alarm_count=curs.execute("select id from alarm_temp where server_id=%s and db_type='%s' and alarm_item='%s' and alarm_type='sms' ;" %(server,db_type,alarm_item))

    if int(alarm_count) >= int(send_sms_max_count) :
        send_sms = 0
    else:
        send_sms = send_sms
   
    curs.close()
    conn.close()
    return send_sms
    
    
def check_db_status(server_id,db_host,db_port,tags,db_type):
    try:
        conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
        conn.select_db(dbname)
        curs = conn.cursor()
        sql="select id from db_status where host='%s' and port=%s " % (db_host, db_port)
        count=curs.execute(sql) 
        if count ==0:
             if db_type=='mysql':
                sort=1
             elif db_type=='oracle':
                sort=2
             elif db_type=='mongodb':            
                sort=3
             elif db_type=='redis':
                sort=4
             else:
                sort=0

             sql="replace into db_status(server_id,host,port,tags,db_type,db_type_sort) values(%s,%s,%s,%s,%s,%s);"
             param=(server_id,db_host,str(db_port),tags,db_type,str(sort))
             curs.execute(sql,param)
             conn.commit()
             
    except Exception,e:
        print "Check db status table: " + str(e) 
    finally:
        curs.close()
        conn.close()          

def update_db_status_init(server_id,role,version,db_host,db_port,tags):
    try:
        conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
        conn.select_db(dbname)
        curs = conn.cursor()
        #sql="replace into db_status(server_id,host,port,tags,version,role,connect) values(%s,%s,%s,%s,%s,%s,%s)"
	#param=(server_id,host,port,tags,version,role,0)
	#curs.execute(sql,param)
        curs.execute("update db_status set role='%s',version='%s',tags='%s' where host='%s' and port='%s';" %(role,version,tags,db_host,db_port))
        conn.commit()
    except Exception, e:
        print "update db status init: " + str(e)
    finally:
      curs.close()
      conn.close()


def update_db_status_more(vals, db_host,db_port=''):
    try:
	setCols = []
	for val in vals:
	    field = val[0]
	    value = val[1]
	    field_tips=field+'_tips'
	    if not cmp("-1",value):
		value_tips='no data'
	    else:
		(alarm_time,alarm_item,alarm_value,alarm_level) = val[2:]
		value_tips="""
			     item: %s\n<br/>
			    value: %s\n<br/> 
			    level: %s\n<br/>
			     time: %s\n<br/> 
			""" %(alarm_item,alarm_value,alarm_level,alarm_time)
	    setCols.append("%s='%s'" % (field, value))
	    setCols.append("%s='%s'" % (field_tips, value_tips))
        if cmp('', db_port) and int(db_port) >0:
            updSql = "update db_status set %s where host='%s' and port='%s';" %(",".join(setCols),db_host,db_port)
        else:
            updSql = "update db_status set %s where host='%s';" %(",".join(setCols),db_host)
        conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
        conn.select_db(dbname)
        curs = conn.cursor()
        curs.execute(updSql)
        conn.commit()
    except Exception, e:
        print "update db status more: " + str(e)
	print db_host,db_port, vals 
    finally:
	curs.close()
	conn.close()




def update_db_status(field,value,db_host,db_port,alarm_time,alarm_item,alarm_value,alarm_level):
    try:
        field_tips=field+'_tips'
        if value==-1:
            value_tips='no data'
        else:
            value_tips="""
                          item: %s\n<br/>
                         value: %s\n<br/> 
                          level: %s\n<br/>
                          time: %s\n<br/> 
                    """ %(alarm_item,alarm_value,alarm_level,alarm_time)

        conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
        conn.select_db(dbname)
        curs = conn.cursor()
        if cmp('', db_port) and int(db_port) >0:
            curs.execute("update db_status set %s='%s',%s='%s' where host='%s' and port='%s';" %(field,value,field_tips,value_tips,db_host,db_port))
        else:
            curs.execute("update db_status set %s='%s',%s='%s' where host='%s';" %(field,value,field_tips,value_tips,db_host))
        conn.commit()
    except Exception, e:
        print "update db status: " + str(e)
	print field,value,db_host,db_port,alarm_time,alarm_item,alarm_value,alarm_level 
    finally:
      curs.close()
      conn.close()


def update_check_time():
    try:
        conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
        conn.select_db(dbname)
        curs = conn.cursor()
        localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        curs.execute("update lepus_status set lepus_value='%s'  where lepus_variables='lepus_checktime';" %(localtime))
        conn.commit()
    except Exception, e:
        print "update check time: " + str(e)
    finally:
      curs.close()
      conn.close()

def flush_hosts():
    conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
    conn.select_db(dbname)
    cursor = conn.cursor()
    cursor.execute('flush hosts;');

def get_mysql_status(cursor):
    data=cursor.execute('show global status;');
    data_list=cursor.fetchall()
    data_dict={}
    for item in data_list:
        data_dict[item[0]] = item[1]
    return data_dict

def get_mysql_variables(cursor):
    data=cursor.execute('show global variables;');
    data_list=cursor.fetchall()
    data_dict={}
    for item in data_list:
        data_dict[item[0]] = item[1]
    return data_dict

def get_mysql_version(cursor):
    cursor.execute('select version();');
    return cursor.fetchone()[0]



##################################### mail ##############################################
mail_host = get_option('smtp_host')
mail_port = int(get_option('smtp_port'))
mail_user = get_option('smtp_user')
mail_pass = get_option('smtp_pass')
mail_send_from = get_option('mailfrom')
def send_mail(to_list,sub,content):
    '''
    to_list:发给谁
    sub:主题
    content:内容
    send_mail("aaa@126.com","sub","content")
    '''
    #me=mail_user+"<</span>"+mail_user+"@"+mail_postfix+">"
    me=mail_send_from
    msg = MIMEText(content, _subtype='html', _charset='utf8')
    msg['Subject'] = "Camelbell %s" % (Header(sub,'utf8'))
    msg['From'] = Header(me,'utf8')
    msg['To'] = ";".join(to_list)
    try:
        smtp = smtplib.SMTP()
        smtp.connect(mail_host,mail_port)
        smtp.login(mail_user,mail_pass)
        smtp.sendmail(me,to_list, msg.as_string())
        smtp.close()
        return True
    except Exception, e:
        print str(e)
        return False


##################################### db_server_os ##############################################
def init_server_os():
    try:
	conn=MySQLdb.connect(host=host,user=user,passwd=passwd,port=int(port),connect_timeout=5,charset='utf8')
	conn.select_db(dbname)
	cursor = conn.cursor()
	#print "disable os monitor"
	cursor.execute("update db_servers_os set monitor = 0")
	conn.commit()

	# insert/update
	dbs = ["mysql", "oracle", "mongodb", "redis"]
	for db in dbs:
	    #print "insert/update %s" % (db)
	    insSql = "insert into db_servers_os (host, tags, monitor) " \
		    + " SELECT HOST,TAGS,max(monitor_os) as dm from db_servers_%s d group by host " % (db) \
		    + " ON DUPLICATE KEY UPDATE monitor=IF(monitor=0, VALUES(monitor), monitor)"
	    cursor.execute(insSql)

	conn.commit()
    except Exception,e:
        print "Fail init_server_os: %s" %(e)
	return False
    finally:
        cursor.close()
        conn.close()          
        

    return True


##################################### salt ##############################################
def doSaltCmd(saltCmd):
    #print saltCmd
    retry = 3
    for i in range(0, retry):
        cmdRes = os.popen(saltCmd).read()
        if re.search('^No minions matched the target', cmdRes):
	    print "No minions matched the target try %s" % (i+1)
            continue
        else:
            return cmdRes.rstrip("\n")
    return None

def exeSaltCmd(ip, cmd):
    sCmd = '''salt --async '%s' cmd.run "%s" ''' % (ip, cmd.replace('"','\\"').replace('$','\\$'))
    # Executed command with job ID: 20160331112308102201
    sRes = doSaltCmd(sCmd)
    if sRes != None:
	sVals = sRes.split()
	jobID = sVals[len(sVals)-1]
	time.sleep(3)
	retry = 3
	for i in range(0, retry):
	    sJobCmd = '''salt-run --out='json' jobs.lookup_jid %s ''' % (jobID)
	    jobRes = doSaltCmd(sJobCmd)
	    #print cmdRes
	    if re.search('^No minions matched the target', jobRes):
		continue
	    else:
		saltRes = json.loads(jobRes).get(ip)
		if re.search('^Minion did not return', str(saltRes)):
		    continue
		else:
		    return saltRes
    return None

def exeSaltAsyncCmd(ip, cmd):
    sCmd = '''salt --async '%s' cmd.run "%s" ''' % (ip, cmd.replace('"','\\"').replace('$','\\$'))
    # Executed command with job ID: 20160331112308102201
    sRes = doSaltCmd(sCmd)
    if sRes != None and cmp('', sRes) and re.search("^Executed", sRes):
	sVals = sRes.split()
	jobID = sVals[len(sVals)-1]
	return jobID
    #print sCmd, sRes
    return None

def getSaltJobByID(ip, jobID):
    retry = 3
    for i in range(0, retry):
	sJobCmd = '''salt-run --out='json' jobs.lookup_jid %s ''' % (jobID)
	jobRes = doSaltCmd(sJobCmd)
	#print cmdRes
	if re.search('^No minions matched the target', jobRes):
	    print "No minions matched the target try %s" % (i+1)
	    continue
	else:
	    saltRes = json.loads(jobRes).get(ip)
	    if re.search('^Minion did not return', str(saltRes)):
		print "Minion did not return try %s" % (i+1)
		continue
	    else:
		return saltRes
    return None

def checkSaltKey(ip):
    if exeSaltCmd(ip, "hostname") != None:
        return True
    else:
        return False

def encode(str):
    return crypt.sub_encrypt(str)

def decode(str):
    return crypt.sub_decrypt(str)

