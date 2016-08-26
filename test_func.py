#!/usr/bin/env python
#coding:utf-8
import os,sys
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("root")
path='./include'
sys.path.insert(0,path)
import functions as func

def main():
    saveMysqlStatus={}
    saveMysqlStatus["role"]="test"
    func.other_save("mysql_status", saveMysqlStatus)

    logger.info("finished.")


if __name__=='__main__':
    main()
    exit(0)

