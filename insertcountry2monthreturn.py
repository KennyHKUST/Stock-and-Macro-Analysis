import MySQLdb,time,os,csv,datetime
conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
errLog=open("err.log","w")
cur=conn.cursor()
sql="SELECT * FROM trweb.month_return;"
number=0
print sql
try:
    cur.execute(sql)
    results=cur.fetchall()
    for row in results:
    	number+=1
    	ric=row[0]
    	ts=row[1]
    	#print ts
    	sqlgetcountry="SELECT * FROM trweb.tr_master_id where ric='"+ric+"'"
    	#print sqlgetcountry
    	try:
   			cur.execute(sqlgetcountry)
			ricinfo=cur.fetchall()
   			for info in ricinfo:
   				country=info[3]
    			sqlin="Update month_return Set country ='" + str(country) +"' where ric='"+str(ric)+"' and ts='"+str(ts)+"'"
    			#print sqlin
    			try:
    				cur.execute(sqlin)
    				conn.commit()
    			except Exception as e:
			    	errLog.write(sqlin+str(e)+'\n')
			    	conn.rollback()
    	except:
    		print str(e)
    	if number%1000==0:
    		print number
except Exception as e:
	print str(e)
conn.close()
errLog.close()