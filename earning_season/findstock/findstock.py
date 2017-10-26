import MySQLdb,time,os,csv,datetime
from datetime import datetime
def findstock(change):
	todayindex=2365048
	#todayindex=0
	errLog = open("err.log","w")
	if change==1:
		fo=open("rics.txt","w")
	else:
		foQ=open("ricsQ.txt","w")
	conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
	cur=conn.cursor()
	numberOfRics=0
	sql="SELECT distinct(ric) FROM trweb.tr_report_quarter"
	print sql
	ricArray=[]
	try:
	    cur.execute(sql)
	    results=cur.fetchall()
	    for row in results:
	    	ric=row[0]
	    	ricArray.append(ric)
	except Exception as e:
	    errLog.write(sql+"\n"+str(e)+"\n")
	    conn.rollback()

	#print ricArray

	for ric in ricArray:
		numberOfRics+=1
		if numberOfRics%1000==0:
			print str(numberOfRics)+', '+str(datetime.now())
		#sql="SELECT * FROM trweb.tr_report_quarter where ric='600792.SS'order by ts desc"
		sql="SELECT * FROM trweb.tr_report_quarter where ric='"+ric+"'order by ts desc"

		try:
		    cur.execute(sql)
		    results=cur.fetchall()
		    number=0
		    revArray=[]
		    incArray=[]
		    tsArray=[]
		    for row in results:
		    	number+=1
		    	if number==1:
		    		pk=row[0]
		    		if pk<todayindex:
		    			break
		    		ts=str(row[2])
		    		ttl_rev=row[3]
		    		net_inc=row[30]
		    		#print ttl_rev
		    		if ttl_rev==None or net_inc==None:
		    			break
		    		#year0=int(ts[0:4])
		    		#slashPosition=ts.find("-")
		    		#month=int(ts[slashPosition+1:7])
		    		#print month
		    		#if month!=9: #will be used in real because we only consider the updated data
		    		#	break
		    		#if year0<2016:
		    		#	break
		    	elif number==2 or number==3 or number==4 or number==5 or number==6 or number==7 or number==8 or number==9:
		    		ts=str(row[2])
		    		ttl_rev=row[3]
		    		net_inc=row[30]
		    		if ttl_rev==None or net_inc==None:
		    			break		
		    	elif number>6:
		    		break
		    	else:
		    		continue
		    	tsArray.append(ts)
		    	revArray.append(float(ttl_rev))
		    	incArray.append(float(net_inc))
		    if len(revArray)<4 or incArray<4:#take into account two indicators
		    	continue
		    #print revArray
		    #print incArray
		    if revArray[3]==0 or revArray[2]==0 or incArray[3]==0 or incArray[2]==0:
		    	continue #avoid division by 0
		    if change==1:
		    	revgrowth0=(revArray[0]-revArray[4])/revArray[4]
		    	revgrowth1=(revArray[1]-revArray[5])/revArray[5]
		    	revgrowth2=(revArray[2]-revArray[6])/revArray[6]
		    	revgrowth3=(revArray[3]-revArray[7])/revArray[7]
		    	revgrowth4=(revArray[4]-revArray[8])/revArray[8]
		    	incgrowth0=(incArray[0]-incArray[4])/abs(incArray[4])
		    	incgrowth1=(incArray[1]-incArray[5])/abs(incArray[5])
		    	incgrowth2=(incArray[2]-incArray[6])/abs(incArray[6])
		    	incgrowth3=(incArray[3]-incArray[7])/abs(incArray[7])
		    	incgrowth4=(incArray[4]-incArray[8])/abs(incArray[8])

		    else:
		    	revgrowth0=(revArray[0]-revArray[1])/revArray[1]
		    	revgrowth1=(revArray[1]-revArray[2])/revArray[2]
		    	revgrowth2=(revArray[2]-revArray[3])/revArray[3]
		    	revgrowth3=(revArray[3]-revArray[4])/revArray[4]
		    	revgrowth4=(revArray[4]-revArray[5])/revArray[5]
		    	incgrowth0=(incArray[0]-incArray[1])/abs(incArray[1])
		    	incgrowth1=(incArray[1]-incArray[2])/abs(incArray[2])
		    	incgrowth2=(incArray[2]-incArray[3])/abs(incArray[3])
		    	incgrowth3=(incArray[3]-incArray[4])/abs(incArray[4])
		    	incgrowth4=(incArray[4]-incArray[5])/abs(incArray[5])


		    if revgrowth0<=revgrowth1 and incgrowth0<=incgrowth1:
		    	continue
		    sqlscreen="Select * from trweb.quarterly_adv_market_cap where ric='"+str(ric)+"'"
		    try:
		    	cur.execute(sqlscreen)
		    	adv_market_cap=cur.fetchall()
		    	for screenData in adv_market_cap:
		    		adv=float(screenData[1])
		    		market_cap=float(screenData[2])
		    		break
		    except Exception as e:
		    	errLog.write(sqlscreen+'\n'+str(e)+'\n')
		    	conn.rollback()
		    if adv-0.5<0:
		    	continue
		    sqlget="Select * from tr_master_id where ric='"+str(ric)+"'"
		    #print sqlget
		    try:
		    	cur.execute(sqlget)
		    	infoResults=cur.fetchall()
		    	for info in infoResults:
		    		exchange=info[4]
		    		#print exchange
		    		name=info[10]
		    		break
		    except Exception as e:
		    	errLog.write(sqlget+'\n'+str(e)+'\n')
		    	conn.rollback()
		    output=ric+"; "+name+"; "+exchange+"; "+str(adv)+"; "+str(market_cap)+"; "+tsArray[0]+"; "
		    #print output
		    if revgrowth0>revgrowth1 and incgrowth0>incgrowth1:
		    	criteriaMet="Both"
		    	if revgrowth0<0 and incgrowth0<0:
		    		sign="-ve -ve"
		    	elif revgrowth0<0 and incgrowth0>0:
		    		sign="-ve +ve"
		    	elif revgrowth0>0 and incgrowth0<0:
		    		sign="+ve -ve"
		    	else:
		    		sign="+ve +ve"
		    else:
		  		criteriaMet="-"
		    #print output
		    #print criteriaMet
		    revTP="-"
		    incTP="-"
		    if revgrowth0>revgrowth1:
		    	if criteriaMet!="Both":
		    		criteriaMet="Revenue Growth"
		    		if revgrowth0<0:
		    			sign="-ve"
		    		else:
		    			sign="+ve"
		    	revTP="0"
		    	if revgrowth1>revgrowth2:
		    		revTP="-1"
		    		if revgrowth2>revgrowth3:
		    			revTP="-2"
		    			if revgrowth3>revgrowth4:
		    				revTP="-3"
		    #print criteriaMet
		    revOutput=revTP+"; "+str(revgrowth0)+'; '+str(revgrowth1)+'; '+str(revgrowth2)+'; '+str(revgrowth3)+'; '+str(revgrowth4)+'; '
		    #print revOutput
		    if incgrowth0>incgrowth1:
		    	if criteriaMet!="Both":
		    		criteriaMet="Income Growth"
		    		if incgrowth0<0:
		    			sign="-ve"
		    		else:
		    			sign="+ve"
		    	incTP="0"
		    	if incgrowth1>incgrowth2:
		    		incTP="-1"
		    		if incgrowth2>incgrowth3:
		    			incTP="-2"
		    			if incgrowth3>incgrowth4:
		    				incTP="-3"
		    incOutput=incTP+"; "+str(incgrowth0)+'; '+str(incgrowth1)+'; '+str(incgrowth2)+'; '+str(incgrowth3)+'; '+str(incgrowth4)
		    #print incOutput
		    #if revgrowth0 or 
		    output+=criteriaMet+'; '+sign+'; '+revOutput+incOutput
		    if change==1:
		    	fo.write(output+'\n')
		    	#print 'written'
		    	# print incArray[4]-incArray[8]
		    	# print incArray[8]
		    else:
		    	#print output
		    	foQ.write(output+'\n')
		    	#print 'written'
		except Exception as e:
		    errLog.write(sql+"\n"+str(e)+"\n")
		    conn.rollback()
	if change==1:
		fo.close()
	else:
		foQ.close()
	conn.close()
	errLog.close()
findstock(0)
findstock(1)