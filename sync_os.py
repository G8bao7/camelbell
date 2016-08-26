#!//bin/env python
#coding:utf-8
import sys
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("main")
path='./include'
sys.path.insert(0,path)
import functions as func

dbhost = func.get_config('monitor_server','host')
dbport = func.get_config('monitor_server','port')
dbuser = func.get_config('monitor_server','user')
dbpasswd = func.get_config('monitor_server','passwd')
dbname = func.get_config('monitor_server','dbname')

def main():
    logger.info("init_server_os start.")
    func.init_server_os()
    logger.info("init_server_os finish.")

if __name__=='__main__':
     main()
