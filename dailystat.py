#!/bin/env python
#coding:utf-8
import os
import sys
import string
import time
import datetime
import MySQLdb
(pathAbs, scName) = os.path.split(os.path.abspath(sys.argv[0]))  
path='%s/include' % (pathAbs)
#path='./include'
sys.path.insert(0,path)

from multiprocessing import Process;
import logging
import logging.config
logging.config.fileConfig("%s/etc/logger.ini" % (pathAbs))
logger = logging.getLogger("main")


def job_run(script_name):
    logger.info("run %s" % (script_name))
    osCmd = "python %s/%s.py > /tmp/%s.log 2>&1" % (pathAbs,script_name, script_name)
    logger.info(osCmd)
    os.system(osCmd)

def main():
    logger.info("camelbell daily stat controller start.")
    
    scripts = ["dailystat_mysql", "check_oracle_awrreport"] 
    joblist = []
    #
    for script_name in scripts:
	job = Process(target = job_run, args = (script_name,))
	joblist.append(job)
	job.start()

    #   
    isalive = True
    while isalive:
        isalive = False
        for proc in joblist:
            if proc.is_alive():
                isalive = True
                break
        time.sleep(3)

    for job in joblist:
	job.join();

    logger.info("camelbell daily stat controller finished.")

  
if __name__ == '__main__':  
    main()
