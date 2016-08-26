#!/usr/bin/env python
# encoding: utf-8
import os,sys,signal,re
import argparse
from datetime import datetime
import MySQLdb
import json
import traceback

import soaplib
#from soaplib.core.util.wsgi_wrapper import run_twisted #发布服务
from soaplib.core.server import wsgi
from soaplib.core.service import DefinitionBase  #所有服务类必须继承该类
from soaplib.core.service import soap  #声明注解
from soaplib.core.model.clazz import Array #声明要使用的类型
from soaplib.core.model.clazz import ClassModel  #若服务返回类，该返回类必须是该类的子类
from soaplib.core.model.primitive import Integer,String 

import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("main")
path='./include'
sys.path.insert(0,path)
import functions as func

incpKey = "inception"
api_host = func.get_config(incpKey,'api_host')
api_port = int(func.get_config(incpKey,'api_port'))
db_host = func.get_config(incpKey,'db_host')
db_port = int(func.get_config(incpKey,'db_port'))
print api_host, api_port, db_host, db_port

'''
========================================================================================
============================== hello world  ======================================
========================================================================================
'''
class C_ProbeCdrModel(ClassModel):
    __namespace__ = "C_ProbeCdrModel"
    Name = String
    Id = Integer

class HelloWorldService(DefinitionBase):
    #声明一个服务，标识方法的参数以及返回值
    @soap(String,_returns = String)
    def say_hello(self,name):
        return 'hello %s!'%name

    #声明一个服务，标识方法的参数以及返回值
    @soap(_returns = String)
    def gl(self):
        return 'Good Luck!'

'''
========================================================================================
============================== inception   ======================================
========================================================================================
'''

#this is a web service
class InceptionService(DefinitionBase):
    INCP_FIELDS_WEB = ('SQL', 'errormessage')
    INCP_FIELDS_FULL = ( 'ID', 'stage', 'errlevel', 'stagestatus', 'errormessage', 'SQL', 'Affected_rows', 'sequence', 'backup_dbname', 'execute_time', 'sqlsha1')
    INCP_ERR_OK = 0 
    INCP_ERR_WARN = 1
    INCP_ERR_CRITICAL = 2

    # _returns = Array(String)
    @soap(String, Integer, String, String, String, String, _returns = String)
    def auditSql(self, pHost, pPort, pUser, pPasswd, pDB, pSql):
        incpSql =  self.transferInception(pHost, pPort, pUser, pPasswd, pDB, pSql)

	webIncps = []
	incps = self.runIncepSql(incpSql)
	for incp in incps:
	    errLv = incp.get("errlevel", self.INCP_ERR_WARN)
	    # error
	    if errLv != self.INCP_ERR_OK:
		webIncp = {}
		for field in self.INCP_FIELDS_WEB:
		    webIncp[field] = incp.get(field)
		webIncps.append(webIncp)
	if len(webIncps) <= 0:
	    res = "OK"
	else:
	    res = webIncps

        resJson = json.dumps(res, sort_keys=True, indent=4, skipkeys=True)
	return resJson

    '''
    return sqlString
    '''
    def transferInception(self, pHost, pPort, pUser, pPasswd, pDB, pSql):
	headers = []
	headers.append("--user=%s" % (pUser))
	headers.append("--password=%s" % (pPasswd))
	headers.append("--host=%s" % (pHost))
	headers.append("--port=%s" % (pPort))

	isPrint = False
	if isPrint:
	    headers.append("--enable-query-print")
	else:
	    headers.append("--disable-remote-backup")
	    headers.append("--enable-check")
       
	incps = []
	incps.append(u"/*%s;*/" % (";".join(headers)))
	incps.append(u"inception_magic_start;")
	incps.append(u"use %s;" % (pDB))
	srcSql = pSql
	if re.search(";$",srcSql):
	    incps.append(srcSql)
	else:
	    incps.append("%s;" % (srcSql))
	incps.append(u"inception_magic_commit;")
	incpSql = "\n".join(incps)

	return incpSql

    def runIncepSql(self, incpSql):
	incp_host = db_ip
        incp_port = db_port
	try:
	    isDict = False
	    if isDict:
		incpConn = MySQLdb.Connect(host=incp_host, port=incp_port, charset="utf8", connect_timeout=2, cursorclass = MySQLdb.cursors.DictCursor)
	    else:
		incpConn = MySQLdb.Connect(host=incp_host, port=incp_port, charset="utf8", connect_timeout=2)
	    cur = incpConn.cursor()
	    ret = cur.execute(incpSql)
	    lines = cur.fetchall()
	    descFields = cur.description
	    cur.close()
	    incpConn.close()

	    incpVals = []
	    if isDict:
		incpVals = lines
	    else:
		fieldNum = len(descFields)
		fieldNames = [i[0] for i in descFields]

		incpVals = []
		for line in lines:
		    incpVal = {}
		    i = 0
		    for val in line:
			incpVal[fieldNames[i]] = val
			i += 1
		    if len(incpVal) > 0:
			incpVals.append(incpVal)

		for incpVal in incpVals:
		    for field in fieldNames:
			key = field
			val = incpVal.get(field)

	    return incpVals
	except MySQLdb.Error,e:
	    traceback.print_exc()
	return None


'''
========================================================================================
============================== service   ======================================
========================================================================================
'''
def service_Hello(host, port):
    targetNamespace = "guoqi"
    soap_app=soaplib.core.Application([HelloWorldService], targetNamespace)
    wsgi_app=wsgi.Application(soap_app)
    isMakeServer = False
    #isMakeServer = True
    if isMakeServer:
	print 'wsdl is at: http://%s:%s/?wsdl' % (host, port)
	try:
	    from wsgiref.simple_server import make_server
	    server = make_server(host, port, wsgi_app)
	    server.serve_forever()
	except ImportError:
	    print 'error'
    else:
	from soaplib.core.util.wsgi_wrapper import run_twisted
	application = "hello3"
	print 'wsdl is at: http://%s:%s/%s/?wsdl' % (host, port, application)
	run_twisted( [(wsgi_app, application)], port)

def service_inception(host, port):
    from soaplib.core.util.wsgi_wrapper import run_twisted

    targetNamespace = "guoqi"
    soap_app = soaplib.core.Application([InceptionService], targetNamespace)
    wsgi_app = wsgi.Application(soap_app)
    application = "sqlaudit"
    print 'wsdl is at: http://%s:%s/%s/?wsdl' % (host, port, application)
    run_twisted( [(wsgi_app, application)], port)

if __name__ == '__main__':
    service_Hello(api_host, api_port)
    service_inception(api_host, api_port)

    exit(0)

