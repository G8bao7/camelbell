#!//bin/env python
#coding:utf-8
import urllib2,json,sys,cookielib,socket,copy,signal,argparse

class zabbixApiClient():
    def __init__(self, zabbixHost, user, passwd):
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/14.0.1',
                        'Referer':''
                        ,'Content-Type':'application/json-rpc'}
        self.url = "http://%s/zabbix/api_jsonrpc.php" % (zabbixHost)
	print self.url
        logIn = {}
        logIn["jsonrpc"] = "2.0"
        logIn["method"] = "user.login"
        logIn["params"] = {"user":user, "password":passwd}
        logIn["id"] = "1"
        logIn["auth"] = None
        
        jsLoginData = json.dumps(logIn, sort_keys=True, indent=4, skipkeys=True)
        cj = cookielib.CookieJar()
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        req = urllib2.Request(self.url, jsLoginData, self.headers)
        content = self.opener.open(req)
        logRes = json.loads(content.read())
        auth = logRes.get("result")
	if auth == None:
	    print self.url
	    print jsLoginData
	    exit(1)
        basicParams = {}
        basicParams["jsonrpc"] = "2.0"
        basicParams["id"] = 1
        basicParams["auth"] = auth
        self.basicParams = basicParams
        
    def callZabbixApi(self, method, params):
        newParams = copy.deepcopy(self.basicParams)
        newParams["id"] =  1
        newParams["method"] = method
        newParams["params"] = params
        jsData = json.dumps(newParams, sort_keys=True, indent=4, skipkeys=True)
        #print jsData
        req = urllib2.Request(self.url, jsData, self.headers)
        content = self.opener.open(req)
        res = json.loads(content.read())
        result = res.get("result", None)
        return result
            
    def getItemByName(self, host, keyName):
        method = "item.get"
        params = {"output": "extend", "host": host, "filter": {"key_": keyName},"sortfield": "name"}
        item = self.callZabbixApi(method, params)
        if item == None or len(item) <= 0:
            return None
        return item[0]
    
    def getItemValue(self, item, sum=1, time_from=None):
        method = "history.get"
        params = {"output":"extend",
                  "itemids":item.get("itemid"),
                  "history":item.get("value_type"),
                  "limit":sum,
                  "sortfield":"clock",
                  "sortorder": "DESC"}
        vals = self.callZabbixApi(method, params)
	if vals != None and len(vals)>0:
	    return vals[0]
	return None
    
    def getLastItemValue(self, ip, itemName):
        item = self.getItemByName(ip,itemName)
        if item == None:
	    #print "Error value:%s,%s" % (ip, itemName)
            return None
        lastVal = self.getItemValue(item)
        return lastVal
    
            

if __name__ == '__main__':
    zbHost = "172.21.100.200"
    zbHost = "172.21.200.27"
    zbUser = "dba"
    zbPasswd = "zx@dba"
    zbAc = zabbixApiClient(zbHost, zbUser, zbPasswd)
     
    aIp = "172.21.100.38"
    import time
    print time.time()
    aaa=zbAc.getLastItemValue(aIp,"vm.memory.size[pavailable]")
    print aaa 




