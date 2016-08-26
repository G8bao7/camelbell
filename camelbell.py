#!/bin/env python
#coding:utf-8
import os
import sys
import string
import time
import datetime
import MySQLdb
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("main")
path='./include'
sys.path.insert(0,path)
import functions as func
from multiprocessing import Process;
  

def job_run(script_name,times=3600):
    logger.info("%s check per %s seconds." % (script_name, times))
    while True:
        os.system("python "+script_name+".py")
        time.sleep(int(times))


def main():
    logger.info("camelbell controller start.")
    monitor = str(func.get_option('monitor'))
    monitor_mysql = str(func.get_option('monitor_mysql'))
    monitor_mysql_bigtable = str(func.get_option('monitor_mysql_bigtable'))
    monitor_mongodb = str(func.get_option('monitor_mongodb'))
    monitor_oracle = str(func.get_option('monitor_oracle'))
    monitor_redis = str(func.get_option('monitor_redis'))
    monitor_os = str(func.get_option('monitor_os'))
    alarm = str(func.get_option('alarm'))
    frequency_monitor = func.get_option('frequency_monitor')
    frequency_monitor_alarm = int(frequency_monitor)+10
    frequency_sync_os = func.get_option('frequency_sync_os')
   
    joblist = []
    if monitor=="1":
	#
	logger.info("[BBQ] sync_os")
	job = Process(target = job_run, args = ('sync_os', frequency_sync_os))
	joblist.append(job)
	job.start()

	logger.info("[BBQ]monitor mysql %s" % (monitor_mysql))
        if monitor_mysql=="1":
            job = Process(target = job_run, args = ('check_mysql',frequency_monitor))
            joblist.append(job)
            job.start()

        if monitor_oracle=="1":
	    time.sleep(3)
            job = Process(target = job_run, args = ('check_oracle',frequency_monitor))
            joblist.append(job)
            job.start()

        if monitor_mongodb=="1":
	    time.sleep(3)
            job = Process(target = job_run, args = ('check_mongodb',frequency_monitor))
            joblist.append(job)
            job.start()

        if monitor_redis=="1":
	    time.sleep(3)
            job = Process(target = job_run, args = ('check_redis',frequency_monitor))
            joblist.append(job)
            job.start()

        if monitor_os=="1":
	    time.sleep(3)
	    freq_os = 60 * 5
            job = Process(target = job_run, args = ('check_os',freq_os))
            joblist.append(job)
            job.start()

        if alarm=="1":
	    time.sleep(3)
            job = Process(target = job_run, args = ('alarm',frequency_monitor_alarm))
            joblist.append(job)
            job.start()    

        for job in joblist:
            job.join();

    logger.info("camelbell controller finished.")
    

  
if __name__ == '__main__':  
    main()
