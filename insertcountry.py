import MySQLdb,time,os,csv,datetime
errLog = open("newerr.log","w")
conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
sql="SELECT * FROM trweb.relative_monthly_return"
print sql
try:
    cur.execute(sql)
    results=cur.fetchall()
    for row in results:
    	ric=row[1]
    	sqlgetcountry="SELECT * FROM trweb.tr_master_id where ric='"+str(ric)+"'"
    	print sqlgetcountry
    	try:
    		cur.execute(sqlgetcountry)
    		results2=cur.fetchall()
    		for row2 in results2:
    			country=row2[3]
    			print country
    			sqlin="INSERT INTO relative_monthly_return (country) VALUES ('"+str(country)+"')"
    			print sqlin
    			exit(0)
    	except Exception as e:
    		errLog.write(sqlgetcountry+"\n"+str(e)+"\n")
    		conn.rollback()
except Exception as e:
    errLog.write(sql+"\n"+str(e)+"\n")
    conn.rollback()
errLog.close()
conn.close()