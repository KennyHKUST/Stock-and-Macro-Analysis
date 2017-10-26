import MySQLdb,time,os,csv,datetime
from datetime import datetime
def Momentum(number):
	if number==4:
		column="two2four_month_return"
	elif number==7:
		column="two2seven_month_return"
	else:
		column="two2twelve_month_return"
	conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
	errLog=open("err.log","w")
	cur=conn.cursor()
	sql="SELECT distinct(ric) FROM trweb.month_return"
	cur.execute(sql)
	ricresults=cur.fetchall()
	numberOfRics=0
	for row1 in ricresults:
		numberOfRics+=1
		if numberOfRics%1000==0:
			print str(numberOfRics)+str(datetime.now())
		ric=row1[0]
		sql="SELECT * FROM trweb.month_return where ric='"+str(ric)+"' order by ts desc;"
		#sql="SELECT * FROM trweb.month_return where ric='0388.HK' order by ts desc;"

		cur.execute(sql)
		results=cur.fetchall()
		returnDic={}
		tsArray=[]
		fourMonDic={}
		for row in results:
			ts=str(row[1])
			tsArray.append(ts)
			month_return=row[2]
			returnDic[ts]=month_return
		i=0
		#print len(returnDic)
		#print len(tsArray)

		while(i<=len(tsArray)-(number+1)):#number is 4, 7 or 12. Represent -2 to -4, -2 to -7, -2 to -12 respectively
			ttlReturn=1
			z=i+2
			#print i
			while(z<=i+number):
				ttlReturn*=float(returnDic[tsArray[z]])
				z+=1
			#fourMonDic[tsArray[i]]=ttlReturn
			#print tsArray[i]
			sqlin="Update month_return Set "+str(column)+"='" + str(ttlReturn) +"' where ric='"+str(ric)+"' and ts='"+str(tsArray[i])+"'"
			#print sqlin
			try:
				cur.execute(sqlin)
				conn.commit()
			except Exception as e:
				errLog.write(sqlin+str(e)+'\n')
				conn.rollback()
			i+=1

	conn.close()
	errLog.close()
Momentum(12)
#print time.time()
print str(datetime.now())
#print datetime.fromtimestamp(time.time())

