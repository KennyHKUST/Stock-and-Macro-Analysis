import MySQLdb,time,os,csv
from dbsettings import *


conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
ric="ONE.V"
indate="2015-10-31"
errLog = open("newerr.log","w")
sqlColData={}
sqlcheck="SELECT * FROM trweb.tr_report_annual where ric='"+str(ric)+"' and ts='"+str(indate)+"'"
print sqlcheck
try:
    cur.execute(sqlcheck)
    results=cur.fetchall()
    for row in results:
        for i in range(4,33):
            sqlColData[sqlcolDict[i-2]]=row[i]
        #   sqlColData.append(row[i])
        #for z in range(0,29):
            if sqlColData[sqlcolDict[i-2]]==None:
        #       sqlColData.insert(z,"NULL")
                sqlColData[sqlcolDict[i-2]]="NULL"
    print sqlColData

except Exception as e:
    errLog.write(sqlcheck+"\n"+str(e)+"\n")
    conn.rollback()
errLog.close()
