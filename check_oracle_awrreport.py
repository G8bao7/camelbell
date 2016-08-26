#!//bin/env python
#coding:utf-8
import os
import sys
import cx_Oracle
import re
import datetime 
import MySQLdb
import logging
import logging.config
logging.config.fileConfig("etc/logger.ini")
logger = logging.getLogger("oracle")
path='./include'
sys.path.insert(0,path)
import functions as func

(scDir, scFileName) = os.path.split(os.path.abspath(sys.argv[0]))
 
SECTOR_LoadProfile = "Load Profile"
SECTOR_SQL_ELAPSED = "SQL ordered by Elapsed Time"
SECTOR_SQL_GETS = "SQL ordered by Gets"
SECTOR_SQL_READS = "SQL ordered by Reads"
SECTOR_SQL_EXECT = "SQL ordered by Executions"
SECTOR_SQL_TEXT = "Complete List of SQL Text"

parseSectorKeys = {}
parseSectorKeys[SECTOR_LoadProfile] = "Instance Efficiency Percentages"
parseSectorKeys[SECTOR_SQL_ELAPSED] = "SQL ordered by CPU Time"
parseSectorKeys[SECTOR_SQL_GETS] = "SQL ordered by Reads"
parseSectorKeys[SECTOR_SQL_READS] = "SQL ordered by Physical Reads"
parseSectorKeys[SECTOR_SQL_EXECT] = "SQL ordered by Parse Calls"

def saveLoadProfile(statTime, server_id, host, port, tags, instance_num, loadProfs):
    tbName = "oracle_awrreport"

    DBCOLUMN_MAP_LOADPROFILE = {}
    DBCOLUMN_MAP_LOADPROFILE["DB Time"] = "db_time"
    DBCOLUMN_MAP_LOADPROFILE["DB CPU"] = "db_cpu"
    DBCOLUMN_MAP_LOADPROFILE["Redo size"] = "redo_size"
    DBCOLUMN_MAP_LOADPROFILE["Logical reads"] = "logical_reads"
    DBCOLUMN_MAP_LOADPROFILE["User calls"] = "user_calls"
    DBCOLUMN_MAP_LOADPROFILE["Executes"] = "executes"
    DBCOLUMN_MAP_LOADPROFILE["Transactions"] = "transactions"

    dbColVals = {}
    dbColVals["server_id"] = server_id
    dbColVals["host"] = host
    dbColVals["port"] = port
    dbColVals["tags"] = tags
    dbColVals["instance_num"] = instance_num
    dbColVals["stat_date"] = statTime
    dbColVals["create_time"] = datetime.datetime.now()
    for itemName, itemVal in loadProfs.iteritems():
	sItemName = (itemName.split("("))[0].strip()
	if re.search("^Logical read", sItemName):
	    sItemName = "Logical reads"
	
	if DBCOLUMN_MAP_LOADPROFILE.has_key(sItemName):
	    dbColVals[DBCOLUMN_MAP_LOADPROFILE.get(sItemName)] = itemVal

    insColNames = []
    insColVals = []
    for colName, colVal in dbColVals.iteritems():
	insColNames.append(colName)
	insColVals.append(colVal)
    insSql = "replace into %s(%s) VALUES(%s) " % (tbName, ",".join(insColNames), ("%s,"*len(insColVals)).rstrip(","))
    func.mysql_exec(insSql, insColVals)


def saveTopsql(statTime, server_id, host, port, tags, instance_num, topSqls):
    tbName = "oracle_slowquery"

    TEXTCOLUMS = ["ID", "Module", "Text"]
    DBCOLUMN_MAP_TOPSQL = {}
    DBCOLUMN_MAP_TOPSQL["ID"] = "sql_id"
    DBCOLUMN_MAP_TOPSQL["Module"] = "module"
    DBCOLUMN_MAP_TOPSQL["Text"] = "sql_text"
    DBCOLUMN_MAP_TOPSQL["Executions"] = "executions"
    DBCOLUMN_MAP_TOPSQL["Elapsed Time (s)"] = "elapsed_time_per_exec"
    DBCOLUMN_MAP_TOPSQL["Gets per Exec"] = "gets_per_exec"
    DBCOLUMN_MAP_TOPSQL["Reads per Exec"] = "reads_per_exec"
    DBCOLUMN_MAP_TOPSQL["Physical Reads"] = "physical_read_reqs"
    DBCOLUMN_MAP_TOPSQL["Elapsed Time %Total"] = "elapsed_pct"

    for sqlID, sqlVal in topSqls.iteritems():
	dbColVals = {}
	dbColVals["server_id"] = server_id
	dbColVals["host"] = host
	dbColVals["port"] = port
	#dbColVals["tags"] = tags
	dbColVals["instance_num"] = instance_num
	dbColVals["stat_date"] = statTime
	dbColVals["create_time"] = datetime.datetime.now()
	# init default
	for key,dbCol in DBCOLUMN_MAP_TOPSQL.iteritems():
	    if TEXTCOLUMS.count(key) > 0:
		iVal = ''
	    else:
		iVal = 0
	    dbColVals[dbCol] = iVal

	#
	for itemName, itemVal in sqlVal.iteritems():
	    if DBCOLUMN_MAP_TOPSQL.has_key(itemName):
		if itemVal == None:
		    continue
		if TEXTCOLUMS.count(itemName) > 0:
		    iVal = itemVal
		else:
		    iVal = float(itemVal.replace(",",""))
		dbColVals[DBCOLUMN_MAP_TOPSQL.get(itemName)] = iVal

	insColNames = []
	insColVals = []
	for colName, colVal in dbColVals.iteritems():
	    insColNames.append(colName)
	    insColVals.append(colVal)
	insSql = "replace into %s(%s) VALUES(%s) " % (tbName, ",".join(insColNames), ("%s,"*len(insColVals)).rstrip(","))
	#print insSql
	#print insColVals
	func.mysql_exec(insSql, insColVals)

def doTopsqlSummary():
    delSql = "delete from oracle_slowquery_summary"
    func.mysql_exec(delSql)
    insSql = '''
	    INSERT INTO oracle_slowquery_summary(host, port, sql_id, first_id, first_time)
	    SELECT q.host,q.port,q.sql_id,q.id,q.stat_date
	    FROM oracle_slowquery q
	    , (SELECT MIN(id) min_id FROM oracle_slowquery t1 GROUP BY HOST, PORT, sql_id) t
	    WHERE q.id=t.min_id
	    '''
    func.mysql_exec(insSql)

# 
def parseLoadProfile(scLines):
    items = {}
    colNames = ["Per Second", "Per Transaction", "Per Exec", "Per Call"]
    for line in scLines[0]:
        lineVals = re.sub("\s+"," ", line).split(":")
        if len(lineVals) != 2:
            continue
        
        key = lineVals[0].strip()
        keyVals = lineVals[1].split()
        
        item = {}
        for i in range(0, len(keyVals)):
            item[colNames[i]] = float(keyVals[i].replace(",",""))
        items[key] = item
    
    ldpfs = {}
    for key, vals in items.iteritems():
        ldpfs[key] = vals.get("Per Second")
    return ldpfs
    
# 
def parseSqlGroup(lines):
    '''
    print "parseSqlGroup"
    for line in lines:
	print line
    '''
    sqlInfos = []
    sqlInfos.extend(re.sub("\s+"," ", lines[0]).split())
    if re.search(r"^Module:", lines[1]):
        sqlInfos.append((lines[1].split(":"))[1].strip())
        sqlTextIdx = 2
    else:
        sqlInfos.append("")
        sqlTextIdx = 1
    
    sqlText = ""
    for line in lines[sqlTextIdx:]:
        sqlText += line
    sqlInfos.append(sqlText)
    return sqlInfos
    
# 
def parseTopsqlSector(scLines, colNames):
    idIdx = colNames.index("ID")
    efLines = []
    for scLine in scLines:
	isBegin = False
        for line in scLine:
            if re.search(r"--", line):
                isBegin = True
                continue
                
            if not isBegin:
                continue
            efLines.append(line)
        
    # 
    sectorTopsqls = {}
    sqlGroup = []
    sqlEnd = False
    for line in efLines:
        if re.search(r"^None", line):
            sqlEnd = True
        elif sqlEnd:
            sqlPropertys = parseSqlGroup(sqlGroup)
            sqlID = sqlPropertys[idIdx]
            oneTopsql = {}
            for i in range(0, len(colNames)):
		colVal = sqlPropertys[i]
		if not cmp("N/A", colVal):
		    colVal = None
                oneTopsql[colNames[i]] = colVal
	        
            sectorTopsqls[sqlID] = oneTopsql
            
            sqlGroup = []
            sqlGroup.append(line)
            sqlEnd = False
        elif not sqlEnd:
            sqlGroup.append(line)
   
    return sectorTopsqls

def parseSqlElapsed(scLines):
    colNames = ["Elapsed Time (s)", "Executions", "Elapsed Time per Exec (s)", "Elapsed Time %Total",  "%CPU", "%IO", "ID", "Module", "Text"]
    elapSqls = parseTopsqlSector(scLines, colNames)
    return elapSqls

def parseSqlGets(scLines):
    colNames = ["Buffer Gets", "Executions", "Gets per Exec", "Buffer Gets %Total", "Elapsed Time (s)", "%CPU", "%IO", "ID", "Module", "Text"]
    getsSqls = parseTopsqlSector(scLines, colNames)
    return getsSqls

def parseSqlReads(scLines):
    colNames = ["Physical Reads", "Executions", "Reads per Exec", "Physical Reads %Total", "Elapsed Time (s)", "%CPU", "%IO", "ID", "Module", "Text"]
    readsSqls = parseTopsqlSector(scLines, colNames)
    return readsSqls

def parseSqlExecutions(scLines):
    colNames = ["Executions", "Rows Processed", "Rows per Exec", "Elapsed Time (s)", "%CPU", "%IO", "ID", "Module", "Text"]
    execsSqls = parseTopsqlSector(scLines, colNames)
    return execsSqls

def parseTopsql(sectors):
    sqlStatistics = {}
    sqlStatistics[SECTOR_SQL_ELAPSED] = "parseSqlElapsed"
    sqlStatistics[SECTOR_SQL_GETS] = "parseSqlGets"
    sqlStatistics[SECTOR_SQL_READS] = "parseSqlReads"
    sqlStatistics[SECTOR_SQL_EXECT] = "parseSqlExecutions"
    
    topSqls = {}
    for sqlType, sqlMethod in sqlStatistics.iteritems():
	parseSqls = eval(sqlMethod)(sectors[sqlType])
	for sqlID, sqlinfo in parseSqls.iteritems():
	    if not topSqls.has_key(sqlID):
		topSqls[sqlID] = sqlinfo
	    else:
		srcSqlinfo = topSqls.get(sqlID)
		for sqlKey, sqlVal in sqlinfo.iteritems():
		    srcSqlinfo[sqlKey] = sqlVal
    return topSqls

# return {key:[sc1,sc2]}
def parseAwrReport(awrFile):
    f=open(awrFile,"r")
    lines = f.readlines()
    f.close()
    sectors = {}
        
    curSector = []
    curSectorKey = None
    nextSectorKey = None
    for line in lines:
        tline = line.strip("\n").lstrip(b'\x0C')
        if nextSectorKey != None and (re.search(r"^%s" % (nextSectorKey), tline) or re.search(r"^%s" % (curSectorKey), tline)):
            if curSectorKey != None:
		if not sectors.has_key(curSectorKey):
		    sectors[curSectorKey] = []
		sectors.get(curSectorKey).append(curSector)
            curSectorKey = None
            curSector = []
            
        curSector.append(tline)
        for key, nextKey in parseSectorKeys.iteritems():
            if re.search(r"^%s" % (key), tline):
                curSectorKey = key
                nextSectorKey = nextKey
                curSector = []
                break
        
    if curSectorKey != None:
        sectors[curSectorKey] = curSector

    return sectors

def exportReport(reportFile, lines):
    f=open(reportFile, "w")
    for line in lines:
	f.write("%s\n" % (line))
    f.close()
    
# return [[host, port, INSTANCE_NUMBER, time, textReportFile, htmlReportFile]]
def createReport(host, port, sid, user, passwd):
    repInfos = []
    try:
	url="%s:%s/%s" % (host,port,sid)
        conn=cx_Oracle.connect(user, passwd,url)
        logger.info("connnect ok")
        curs=conn.cursor()
        # dbid
        curs.execute("SELECT DBID FROM v$DATABASE")
        lines = curs.fetchall()
        dbID = lines[0][0]
        logger.info("v$DATABASE ok")
 
        # snapshot
	endTime = datetime.date.today()
        qTime = endTime + datetime.timedelta(-1)
        #qSql = "select INSTANCE_NUMBER, MIN(SNAP_ID), MAX(SNAP_ID), MIN(begin_interval_time), MAX(begin_interval_time) from DBA_HIST_SNAPSHOT where begin_interval_time < trunc(:1+1) and begin_interval_time >= trunc(:2) GROUP BY INSTANCE_NUMBER"
        qSql = "select INSTANCE_NUMBER, MIN(SNAP_ID)-1, MAX(SNAP_ID), MIN(begin_interval_time), MAX(begin_interval_time) from DBA_HIST_SNAPSHOT where begin_interval_time >= :1 and begin_interval_time < :2  GROUP BY INSTANCE_NUMBER"
        curs.execute(qSql, (qTime, endTime))
        snaps = curs.fetchall()
	logger.info("awsreport DBA_HIST_SNAPSHOT ok")

	todayReportPath = "%s/awr/oracle/%s" %(scDir, qTime.strftime("%Y-%m-%d"))
	if len(snaps)>0 and (not os.path.exists(todayReportPath)):
	    os.mkdir(todayReportPath)
	#
	for snap in snaps:
	    (insNum, minSnapId, maxSnapId, minSnapTime, maxSnapTime) = snap
	    qSql = "Select * from table(dbms_workload_repository.awr_report_text(:1,:2,:3,:4,0))"
	    curs.execute(qSql, (dbID, insNum, minSnapId, maxSnapId))
	    textLines = curs.fetchall()
	    logger.info("awsreport text ok")
	    textFile = "/tmp/awr/awr-%s-%s-%s.text" % (host, port, insNum)
	    exportReport(textFile, textLines)

	    qSql = "Select * from table(dbms_workload_repository.awr_report_html(:1,:2,:3,:4,0))"
	    curs.execute(qSql, (dbID, insNum, minSnapId, maxSnapId))
	    htmlLines = curs.fetchall()
	    htmlFile = "%s/%s-%s-%s.html" %(todayReportPath, host, port, insNum)
	    exportReport(htmlFile, htmlLines)
	    logger.info("awsreport html ok:%s " %(htmlFile))

	    repInfos.append([host, port, insNum, qTime, textFile, htmlFile])

	curs.close()
	conn.close()

	return (True, repInfos)
    except Exception, e:
        print e
    finally:
	pass

    return (False, '')

def main():
    #host = "192.168.1.175"
    doHosts = ["172.21.100.40", "172.21.100.121", "172.21.100.122"]
    doHosts = ["172.21.100.88"]

    qSql = '''
	SELECT d.id,d.host,d.port,d.dsn,d.tags,s.id FROM db_servers_oracle d
	LEFT JOIN oracle_status s ON d.`host`=s.`host`AND d.`port`=s.`port` AND s.`database_role`='PRIMARY'
	WHERE is_delete=0 AND monitor=1 AND awrreport=1
	HAVING s.id IS NOT NULL
	'''
    servers=func.mysql_query(qSql)
    logger.info("check oracle awsreport started.")

    cnfKey = "monitor_oracle"
    username = func.get_config(cnfKey,'user')
    password = func.get_config(cnfKey,'passwd')
    errSrvs = []
    if servers:
        for row in servers:
	    (server_id, host, port, dsn, tags, stid) = row
	    if doHosts.count(host) <= 0:
		#continue
		pass
	    logger.info("AwrReport:%s,%s,%s" % (host, port, dsn))
	    (isOk, reports) = createReport(host, port, dsn, username, password)
	    if not isOk:
		logger.error("Err createReport:%s,%s,%s" % (host, port, dsn))
		errSrvs.append([server_id, host,port, tags])
		continue
	   
	    for report in reports:
		(host, port, instance_num, statTime, textReportFile, htmlReportFile) = report
		sectors = parseAwrReport(textReportFile)

		## LoadProfile
		lpVals = parseLoadProfile(sectors[SECTOR_LoadProfile])
		logger.info("parseLoadProfile OK")
		# save db
		saveLoadProfile(statTime, server_id, host, port, tags, instance_num, lpVals)

		## TOP SQL
		topSqls = parseTopsql(sectors)
		logger.info("parseTopsql OK")
		# save db	
		saveTopsql(statTime, server_id, host, port, tags, instance_num, topSqls)

    #    
    doTopsqlSummary()

    # check err
    db_type = "oracle"
    create_time = datetime.datetime.now()
    alarm_item = "oracle AwrReport"
    alarm_value = "Fail"
    level = "warning"
    message = ""
    for errSrv in errSrvs:
	(server_id, db_host, db_port, tags) = errSrv
	func.add_alarm(server_id,tags,db_host,db_port,create_time,db_type,alarm_item,alarm_value,level,message)

def test():
    textReportFile = "/tmp/awr-172.21.100.40-1588-1.text"
    sectors = parseAwrReport(textReportFile)
    for lines in sectors[SECTOR_SQL_ELAPSED]:
	for line in  lines:
	    print line
    '''
    SECTOR_SQL_ELAPSED = "SQL ordered by Elapsed Time"
    SECTOR_SQL_GETS = "SQL ordered by Gets"
    SECTOR_SQL_READS = "SQL ordered by Reads"
    SECTOR_SQL_EXECT = "SQL ordered by Executions"
    '''
    topSqls = parseTopsql(sectors)
    #topSqls = parseSqlExecutions(sectors[SECTOR_SQL_EXECT])
    #topSqls = parseSqlReads(sectors[SECTOR_SQL_READS])
    #topSqls = parseSqlGets(sectors[SECTOR_SQL_GETS])
    #topSqls = parseSqlElapsed(sectors[SECTOR_SQL_ELAPSED])
    for sid, sval in topSqls.iteritems():
	#print sval
	continue
	for key, val in sval.iteritems():
	    if not cmp('Text', key):
		continue
	    #print "%s--%s" % (key,val)
    statTime = datetime.datetime.now()
    statTime = datetime.date.today()
    (server_id, host, port, tags, instance_num) = (1,'172.21.100.40',1588,'tetl',1)
    saveTopsql(statTime, server_id, host, port, tags, instance_num, topSqls)

if __name__=='__main__':
    main()
    #test() 
    exit(0)
