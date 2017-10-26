import os,csv,time
#fo=open("problemric.log","w")
errLog = open("newerr.log","w")
ttlnumber=0
rootdir="/Volumes/WD Elements/MarketCap 1-50000"
filefolder=os.listdir(rootdir)
for file in filefolder:
	if file[len(file)-1] !="v":
		continue
	filepath=os.path.join(rootdir,file)

	with open(filepath,"rU") as csvfile:
		spamreader=csv.reader(csvfile, delimiter=",",quotechar='"')
		number=0
		right=0
		ttlnumber+=1
		for row in spamreader:
			number+=1
			if number==1:
            	ric=row[0]
                if row[1] == "#N/A" or row[1] == "Unable to collect data for all requested fields." or row[1] == "Retrieving...":
                    #fo.write(ric+row[1]+'\n')
                    break
                continue
        	try:
	            date=row[2]
	            market_cap=row[3]
	        except Exception as e:
	        	errLog = open("newerr.log","w")
	        	errLog.write(ric+"\n"+str(e)+"\n")

	if ttlnumber%1000 == 0:
		print str(time.clock())+"finishing handing"+str(ttlnumber)+'\n'

fo.close()
errLog.close()