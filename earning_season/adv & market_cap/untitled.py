import MySQLdb,time,os,csv,datetime
conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
errLog=open("errLog.log","w")
filepath="/Users/lixinran/Documents/LiXi/earning_season/adv & market_cap/ric with avt.csv"
with open(filepath,"rU") as csvfile: #loop to open CSV files in the folder
	spamreader=csv.reader(csvfile, delimiter=",",quotechar='"') #obtain the content of csv file, saving by rows
	number=0 #count how many rows have been read
	for row in spamreader:
		number+=1
		if number==1:
			continue
		ric=row[0]
		adv=row[1]
		market_cap=row[2]
		if market_cap=='NULL':
			market_cap=0
		sql="INSERT INTO quarterly_adv_market_cap (ric, adv , market_cap) VALUES ("+"'"+str(ric)+"','"+str(adv)+"','"+str(market_cap)+"')"
		try:
			cur.execute(sql)
			conn.commit()
		except Exception as e:
			errLog.write(sql+"\n"+str(e)+"\n")
			conn.rollback
conn.close
errLog.close
