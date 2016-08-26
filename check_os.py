#!//bin/env python
#coding:utf-8
import os
import sys
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("os")
path='./include'
sys.path.insert(0,path)
import functions as func

(scDir, scFileName) = os.path.split(os.path.abspath(sys.argv[0]))
(scName, scFormat) = scFileName.split(".")

def main():
    monitor_os_method = func.get_option("monitor_os_method")
    cmdFile = "%s/check_os_%s.py" % (scDir, monitor_os_method)
    os.system("python %s" % (cmdFile))

    osTables = ('os_status', 'os_net', 'os_disk', 'os_diskio')
    for osTable in osTables:
	func.mysql_exec('DELETE s FROM %s AS s WHERE s.ip NOT IN (SELECT v.host FROM v_monitor_host AS v)' % (osTable))

if __name__=='__main__':
    main()
