import MySQLdb,time,os,csv
columns=["ric","cusip","sedol",'isin','type_of_equity','country_of_exchange',
'exchange','ticker','mifld','sharish','mkt_cap','currency','country_of_issuer',
'sector','industry']
conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
filepath="/Users/lixinran/Documents/LiXi/master_id_file.csv"
with open(filepath,"rb") as csvfile:
	spamreader=csv.reader(csvfile, delimiter=",",quotechar='"')
	number=0
	for row in spamreader:
		number+=1
		if number==1:
			continue
		name=row[0]
		ric=row[1]
		cusip=row[2]
		sedol=row[3]
		isin=row[4]
		typeOfEquity=row[5]
		countryOfExchange=row[6]
		exchange=row[7]
		ticker=row[8]
		mifld=row[9]
		shariah=row[10]
		mkt_cap=row[11]
		currency=row[12]
		countryOfIssuer=row[13]
		sector=row[14]
		industry=row[15]
		sub_industry=row[16]

		sql="INSERT INTO trweb.new_master_id("
		for column in columns:
			sql+=str(column)+', '
		
		sql+="sub_industry) Values("

		for i in range(1,16):
			if row[i]=='--':
				row[i]='NULL'
				sql+=str(row[i])+", "
			else:
				sql+="'"+str(row[i])+"',"

		sql+="'"+str(row[16])+"')"
		#print sql
		#'ric, country, sector, announce_date, announce_return, mkt_cap) Values('"+str(name)+"','"+str(ric)+"','"+str(cusip)+"','"+str(sedol)+"','"+str(isin)+"','"+str(typeOfEquity)+"')"
		# sql="INSERT INTO VALUES (number,Ric,Latest_Price,Latest_Volume,Market_Cap)"
		try:
		    cur.execute(sql)
		    conn.commit()
		except Exception as e:
			print sql+str(e)
			conn.rollback()
conn.close()


