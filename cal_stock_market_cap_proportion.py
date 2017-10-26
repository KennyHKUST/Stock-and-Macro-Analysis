import MySQLdb,time,os,csv,datetime
errLog = open("newerr.log","w")
conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
#test starts here
'''
sqlget="SELECT trweb.daily_market_cap.ric, ts, trweb.daily_market_cap.market_cap, country FROM trweb.daily_market_cap join tr_master_id on trweb.daily_market_cap.ric=tr_master_id.ric where country='Canada'"
	#print sqlget
Market_CapDic={}
if 'Canada' not in Market_CapDic:
		Market_CapDic['Canada']={}
try:
	cur.execute(sqlget)
	results=cur.fetchall()
	for row in results:
		#print row[0]
		ric=row[0]
		ts=str(row[1])
		marketCap=row[2]
		year=ts[0:4]
		#print ts
		slashPosition=ts.find("-")
		#print slashPosition
		month=ts[slashPosition:7]
		YearMonth=year+month
		#print YearMonth
		if ric not in Market_CapDic['Canada']:
			Market_CapDic['Canada'][ric]={}
		Market_CapDic['Canada'][ric][YearMonth]=marketCap			
		#print Market_CapDic
		
		#	for
		#print Market_CapDic 
except Exception as e:
    errLog.write(sqlget+"\n"+str(e)+"\n")
    conn.rollback()
#print Market_CapDic['Canada']
for ric in Market_CapDic['Canada']:
		#ricTsMCDic[ric]={}		
		for date in Market_CapDic['Canada'][ric]:
			outyear=date[0:4]
			print ric
			print date
			print outyear
			outmonth=date [slashPosition+1:7]
			print outmonth
exit(0)
'''
#obtain all the contries in sql
countryMarket_Cap={}
sql="SELECT distinct(country) FROM trweb.daily_market_cap join tr_master_id on trweb.daily_market_cap.ric=tr_master_id.ric;"
print sql
try:
    cur.execute(sql)
    results=cur.fetchall()
    for row in results:
    	countryMarket_Cap[row[0]]=[]#initialize a dictionary, country->total country market cap
except Exception as e:
    errLog.write(sql+"\n"+str(e)+"\n")
    conn.rollback()
Market_CapDic={}
for country in countryMarket_Cap:
	#obtain daily market cap of all stocks within a country
	sqlget="SELECT trweb.daily_market_cap.ric, ts, trweb.daily_market_cap.market_cap, country FROM trweb.daily_market_cap join tr_master_id on trweb.daily_market_cap.ric=tr_master_id.ric where country='"+country+"'"
	print sqlget
	if country not in Market_CapDic:
		Market_CapDic[country]={}
	try:
		cur.execute(sqlget)
		results=cur.fetchall()
		for row in results:
			#print row[0]
			ric=row[0]
			ts=str(row[1])
			marketCap=row[2]
			year=ts[0:4]
			#print ts
			slashPosition=ts.find("-")
			#print slashPosition
			month=ts[slashPosition:7]
			YearMonth=year+month
			#print YearMonth
			if ric not in Market_CapDic[country]:
				Market_CapDic[country][ric]={}
			Market_CapDic[country][ric][YearMonth]=marketCap			
			#print Market_CapDic
			
			#	for
			#print Market_CapDic 
	except Exception as e:
	    errLog.write(sqlget+"\n"+str(e)+"\n")
	    conn.rollback()
countryTtlMCDic={}
#sql country market cap group by year and month
sqlCMC="SELECT Month(ts),Year(ts),country,Sum(trweb.daily_market_cap.market_cap) FROM trweb.daily_market_cap join tr_master_id on trweb.daily_market_cap.ric=tr_master_id.ric Group by month(ts),year(ts),country"
print sqlCMC
try:
    cur.execute(sqlCMC)
    results=cur.fetchall()
    for row in results:
    	monthindex=row[0]
    	if monthindex<10:
    		monthindex='0'+str(monthindex)
    	else:
    		monthindex=str(monthindex)
    	yearindex=str(row[1])
    	TtlMCYearMonth=yearindex+"-"+monthindex
    	countryindex=row[2]
    	countryTtlMC=row[3]
    	#initialize a dictionary if it hasn't been initialized before
    	if countryindex not in countryTtlMCDic:
    		countryTtlMCDic[countryindex]={}
    	countryTtlMCDic[countryindex][TtlMCYearMonth]=countryTtlMC
    	
except Exception as e:
	    errLog.write(sqlCMC+"\n"+str(e)+"\n")
	    conn.rollback()
ricTsMCDic={}
#print Market_CapDic
#exit(0)
for country in Market_CapDic:
	for ric in Market_CapDic[country]:
		ricTsMCDic[ric]={}		
		for date in Market_CapDic[country][ric]:
			outyear=date[0:4]
			#print outyear
			outmonth=date[slashPosition+1:7]
			#print outmonth
			MC_Proportion=float(Market_CapDic[country][ric][date])/float(countryTtlMCDic[country][date])
			sqlgetReturn="SELECT * FROM trweb.month_return where Ric='"+str(ric)+"'and year(ts)='"+str(outyear)+"' and month(ts)='"+str(outmonth)+"'"
			print sqlgetReturn
			try:
				#print "right"
				cur.execute(sqlgetReturn)
				results=cur.fetchall()
				#print "yes"
				for row in results:
					if row[2] or row[1]=="":
						break
			    	indate=row[1]
			    	print row[1]
			    	MonthReturn=row[2]
			    	print row[2]
			    	RelaReturn=MC_Proportion*float(MonthReturn)
			    	print RelaReturn
			    	#if RelaReturn>1:#it's impossible to have relative return greater than 1
			    	#	continue
			    	sqlin="INSERT INTO relative_return (ric,ts ,relative_return,country) VALUES ("+"'"+str(ric)+"','"+str(indate)+"','"+str(RelaReturn)+"','"+str(country)+"')"
			    	print sqlin
			    	try:
			    		cur.execute(sqlin)
			    		conn.commit()
			    	except:
			    		errLog.write(sqlin)
			    		conn.rollback()
			except Exception as e:
			    errLog.write(sqlgetReturn+"\n"+str(e)+"\n")
			    conn.rollback()
			
			#print RelaReturn
			
			#print sqlin

#print ricTsMCDic


#print countryTtlMCDic['Japan']['2014-09']
errLog.close()
conn.close()