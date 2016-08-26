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
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("root")
path='./include'
sys.path.insert(0,path)
import functions as func
from multiprocessing import Process;
from pymongo import MongoClient

#def check_socialshop(host,port,user="socialshopsim",passwd="giXQnrYRBtXgyz4GlBwx"):
def check_socialshop(host,port,user="imrouter",passwd="HuQEzNbn9DE7KgLTGGRE"):
    print "check_mongodb %s:%s, %s/%s" % (host,port,user,passwd)
    try:
	'''
        connect = pymongo.Connection(host,int(port))
	print connect
        db = connect['admin'] 
        db.authenticate(user,passwd,mechanism='SCRAM-SHA-1')
	'''
	#connect = MongoClient(host,int(port),replicaset='socialshop')
	connect = MongoClient(host,int(port))
        db = connect.imrouter
	print db
	try:
		connect.imrouter.authenticate(user,passwd,mechanism='SCRAM-SHA-1')
		content = db.imrouter.find().limit(2)
		for i in content:
		    print i
	except Exception, e:
		logger.warning(e)

	finally:
		pass
	connect.close()
    except Exception, e:
        logger_msg="check mongodb %s:%s,%s/%s : %s" %(host,port,user,passwd,e)
        logger.warning(logger_msg)

    finally:
	pass



def check_mongodb(host,port,user,passwd):
    print "check_mongodb %s:%s, %s/%s" % (host,port,user,passwd)
    try:
	'''
        connect = pymongo.Connection(host,int(port))
	print connect
        db = connect['admin'] 
        db.authenticate(user,passwd,mechanism='SCRAM-SHA-1')
	'''
	url = "mongodb://%s:%s@%s:%s/%s?authMechanism=MONGODB-CR" % (user,passwd,host,port,'admin')
	url1 = "mongodb://%s:%s@172.18.150.102:%s/%s?authMechanism=MONGODB-CR" % (user,passwd,port,'admin')
	url2 = "mongodb://%s:%s@172.18.150.103:%s/%s?authMechanism=MONGODB-CR" % (user,passwd,port,'admin')
	url1 = "172.18.150.102:%s" % (port)
	url2 = "172.18.150.103:%s/?replicaset=socialshop" % (port)
	url1 = "172.21.100.70:%s" % (port)
	url2 = "172.21.100.167:%s/?replicaset=socialshop" % (port)
	url = "mongodb://%s,%s" % (url1,url2)
	#print url
	#connect = MongoClient(url)
	#connect = MongoClient(host,int(port),replicaset='socialshop')
	connect = MongoClient(host,int(port))
        db = connect.db_gq
	print db
	try:
		connect.db_gq.authenticate(user,passwd,mechanism='SCRAM-SHA-1')
		'''
		bsCmd = bson.son.SON([('serverStatus', 1), ('repl', 2)])
		serverStatus=connect.admin.command(bsCmd)
		#print serverStatus.keys()
		time.sleep(1)
		'''
		db.c_gq.insert_one({'id':1,'name':'ss_%s'%(host),'sex':'maleaa'})

		content = db.c_gq.find()
		for i in content:
		    print i
	except Exception, e:
		logger.warning(e)
		db.c_gq.insert_one({'id':1,'name':'fail_%s'%(host),'sex':'male'})

	finally:
		pass
	connect.close()
    except Exception, e:
        logger_msg="check mongodb %s:%s,%s/%s : %s" %(host,port,user,passwd,e)
        logger.warning(logger_msg)

    finally:
	pass



def main():
	port=10001
	port=10041
	user="admin"
	passwd="admin"
	hosts=["172.18.150.102","172.18.150.103"]
	hosts=["172.18.150.202","172.18.150.103"]
	hosts=["172.21.100.70","172.21.100.167","172.16.100.68"]
	port=10041
	user="ugq"
	passwd="pgq"
	for host in hosts:
		check_mongodb(host,port,user,passwd)
		#check_socialshop(host,port)
		break


if __name__=='__main__':
    main()
