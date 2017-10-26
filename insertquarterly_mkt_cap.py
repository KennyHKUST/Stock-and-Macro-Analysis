import MySQLdb,time,os,csv,datetime #import libraries

conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
filepath="/Users/lixinran/Documents/us_earning_annnounce_return.csv"

with open(filepath,"rb") as csvfile:
	spamreader=csv.reader(csvfile, delimiter=",",quotechar='"')
	number=0
	for row in spamreader:
		number+=1
		ric=row[4]
		if row[11]=='#N/A':
			continue
		mkt_adjusted=float(row[11])

		sql="Update trweb.earnings_announcement_return Set mkt_adjusted='"+str(mkt_adjusted)+"' where ric='"+str(ric)+"'"
		# sql="INSERT INTO VALUES (number,Ric,Latest_Price,Latest_Volume,Market_Cap)"// values('mkt_cap')
		try:
		    cur.execute(sql)
		    conn.commit()
		except Exception as e:
			print sql+'\n'+str(e)
			conn.rollback()

conn.close()


