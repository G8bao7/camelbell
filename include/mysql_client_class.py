#!/usr/bin/env python
# coding=utf-8

import sys
import time
import re
import MySQLdb
import MySQLdb.cursors
import traceback
import functions as func

class mysqlClient():
    def __init__(self, host, port, user=func.get_config("monitor_mysql","user"), password=func.get_config("monitor_mysql","passwd"), db='mysql'):
        self.ip = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.connect = None
        #
        try:
            self.connect = MySQLdb.connect(host=self.ip, user=self.user, passwd=self.password, db=self.db, port=int(self.port), charset="utf8", cursorclass = MySQLdb.cursors.DictCursor, connect_timeout=2)
        except MySQLdb.Error, e:
            self.connect = None
            traceback.print_exc()
            return None
    
    def __delete__(self):
        if self.connect != None:
            try:
                self.connect.rollback()
            except MySQLdb.Error, e:
                traceback.print_exc()
                pass
            finally:
                self.connect.close()


    def isValid(self):
        res = True
        if self.connect == None:
            res = False
        return res

    def getVariables(self, varis):
        res = {}
        if len(varis) <= 0:
            return res

        sqlstr = "SHOW GLOBAL VARIABLES where variable_name in ('%s');" %("','".join(varis))
        rows = self.doSelSql(sqlstr)
        if rows != None:
            for row in rows:
                if row["Value"].isdigit():
                    res[row["Variable_name"]] = int(row["Value"])
                else:
                    res[row["Variable_name"]] = row["Value"]
        return res


    def getStatuss(self, varis):
        if len(varis) <= 0:
            return

        sqlstr = "SHOW GLOBAL STATUS where variable_name in ('%s');" %("','".join(varis))

        rows = self.doSelSql(sqlstr)
        res = {}
        if rows != None:
            for row in rows:
                if row["Value"].isdigit():
                    res[row["Variable_name"]] = int(row["Value"])
                else:
                    res[row["Variable_name"]] = row["Value"]
        return res


    def doSelSql(self, sqlstr, params=[]):
        lines = None
        if self.connect == None:
            return lines
        try:
            cursor = self.connect.cursor()
            if len(params) == 0:
                cursor.execute(sqlstr)
            else:
                cursor.execute(sqlstr, params)
            lines = cursor.fetchall()
            cursor.close()
        except MySQLdb.Error, e:
            lines = None
            traceback.print_exc()
        finally:
            if not self.connect == None:
                self.connect.rollback()

        return lines



