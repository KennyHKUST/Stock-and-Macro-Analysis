import MySQLdb,time,os,csv,datetime

'''
tmpTS = "10/8/2015"

year=tmpTS[-4:]
print year
slashPosition=tmpTS.find("/")
print slashPosition
month=tmpTS[0:slashPosition]
print month
YearMonth=year+"-"+month
print YearMonth
YearMonthDic={}
number=0
YearMonthArray=["2015-10","2016-8"]
print YearMonthArray
YearMonthDic[YearMonth]=[]


#print YearMonthDic[YearMonth][0]
YearMonthDic[YearMonth].append("3.12")

#if YearMonthDic[YearMonth]!=[]:
	#YearMonthDic[YearMonth].append("4.45")
YearMonth="2008-12"
YearMonthDic[YearMonth]=[]
YearMonthDic[YearMonth].append("3.33")

print YearMonthDic
for i in YearMonthDic:
	print YearMonthDic[i]

exit(0)

MonthlyTR=1
day=0
numOfDays=len(YearMonthDic[YearMonthArray[number]])
print numOfDays

while(day<numOfDays):
	x=float(YearMonthDic[YearMonthArray[number]][day])
	MonthlyTR=MonthlyTR*(1+x/100)
	day+=1
print MonthlyTR
exit(0)
'''


insertnumber=0
conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
#fo=open("null.log","w")
#foappend=open("append.log","w")
errLog = open("newerr.log","w")
fo=open("nullric.log","w")
fo2=open("problemric.log","w")
ttlnumber=0
rootdir="/Users/lixinran/Documents/LiXi/Morereturn"
#
filefolder=os.listdir(rootdir)
for file in filefolder:
	if file[len(file)-1] !="v":
		continue
	filepath=os.path.join(rootdir,file)
	#print filepath
	with open(filepath,"rb") as csvfile:
		spamreader=csv.reader(csvfile, delimiter=",",quotechar='"')
		number=0
		ttlnumber+=1
		YearMonthDic={}
		YearMonthArray=[]
		MonthlyTRDic={}
		for row in spamreader:
			number+=1
			if number==1:
				ric=row[0]
				if row[1]=="#N/A":
					fo.write(ric+"\n")
					break
				if row[1]=="Server response is not valid.":
					fo2.write(ric+"\n")
					break
				if row[1]=="Unable to resolve and collect data for all requested identifiers and fields.":
					fo2.write(ric+"\n")
					break
				print ric
				continue
			tmpTS=row[2]
			dailyReturn=row[3]
			if tmpTS=="" or dailyReturn=="":
				number-=1
				continue
			year=tmpTS[-4:]
			slashPosition=tmpTS.find("/")
			#print slashPosition
			month=tmpTS[0:slashPosition]
			YearMonth=year+"-"+month+"-1"
			#print YearMonth			
			#print dailyReturn
			YearMonthArray.append(YearMonth)
			if number!=2:
				#When the month changes, calculate the return of last month 
				if YearMonthArray[number-2]!=YearMonthArray[number-3]:
					day=0
					numOfDays=len(YearMonthDic[YearMonthArray[number-3]])
					MonthlyTR=1
					while(day<numOfDays):#calculate the monthly return by mutiplying each daily return
						returnOfADay=float(YearMonthDic[YearMonthArray[number-3]][day])/100
						MonthlyTR=MonthlyTR*(1+returnOfADay)
						day+=1
					MonthlyTRDic[YearMonthArray[number-3]]=MonthlyTR
					YearMonthDic[YearMonth]=[]
				YearMonthDic[YearMonth].append(dailyReturn)
			else:
				YearMonthDic[YearMonth]=[]
				YearMonthDic[YearMonth].append(dailyReturn)

		for insertmonth in MonthlyTRDic:

			insertTR=MonthlyTRDic[insertmonth]
			sql="Insert into Monthly_Return Values('"+str(ric)+"','"+str(insertmonth)+"','"+str(insertTR)+"')"
			#print insertmonth
			#print insertTR
			#print sql
			try:
				cur.execute(sql)
				conn.commit()
			except Exception as e:
				errLog.write(sql+"\n"+str(e)+"\n") 
				conn.rollback()
				
conn.close()
errLog.close()
fo.close()
fo2.close()
			

				












