# -*- coding: UTF-8 -*-
import MySQLdb,time,os,csv

conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
fo=open("null.log","w")
errLog = open("newerr.log","w")
ttlnumber = 0 
#rootdir="/Users/lixinran/Documents/LiXi/test_price_data"
rootdir="/Volumes/Seagate Backup Plus Drive/Daily Excel/Daily Data/Adjusted Price 50001-102187"
filefolder=os.listdir(rootdir)
for file in filefolder:
    ttlnumber = ttlnumber+1
    if file[len(file)-1] !="v":
        continue
    filepath=os.path.join(rootdir,file)
    #print filepath
    with open(filepath,"rU") as csvfile:
        spamreader=csv.reader(csvfile, delimiter=",",quotechar='"')
        number=0
        numCol=0
        dateArray = []
        YearMonthArray=[]
        price_Array=[]
        month_priceDic={}
        indateArray=[]
        for row in spamreader:
            #print row
            number+=1
            if number==1:
                ric=row[0]
                if row[1] == "#N/A" or row[1] == "Unable to collect data for all requested fields." or row[1] == "Retrieving...":
                    fo.write(ric+'\n')
                    break
                continue
            try:
                date=row[2]
                price=row[3]
            except:
                break

            price_Array.append(price)
            #print date
            #if One_month_return=="":
                #continue
            if date=="":
                break


            #print ric
            #tmpTS = time.strptime(date,"%m/%d/%Y") 
            tmpTS = time.strptime(date,"%m/%d/%Y")
            datestr = time.strftime("%Y-%m-%d",tmpTS)
            dateArray.append(datestr)#date structure
            year=date[-4:]
            slashPosition=date.find("/")
            #print slashPosition
            month=date[0:slashPosition]
            YearMonth=year+"-"+month+"-1"
            YearMonthArray.append(YearMonth)
            #if current date is the same as the last date, this means 
            #the data is repeated, so ignore the row
            if dateArray[number-2]==dateArray[number-3]:
                continue
            if YearMonthArray[number-2]!=YearMonthArray[number-3]:
                monthEnddate=dateArray[number-3]
                indateArray.append(monthEnddate)
                month_price=price_Array[number-3]
                month_priceDic[monthEnddate]=month_price
                
                #print sql
                
                #exit(0)
            else:
                continue
            #try:
              #  cur.execute(sql)
              #  conn.commit()
           # except Exception as e:
             #   errLog.write(sql+"\n"+str(e)+"\n")
             #   conn.rollback()
    curnum=0
    for date in indateArray:
        curnum+=1
        if curnum<len(indateArray):
            #print ric
            indate=indateArray[curnum]
            if float(month_priceDic[indateArray[curnum-1]])==0:#ignore the case if price is 0
                continue
            in_price=float(month_priceDic[indateArray[curnum]])/float(month_priceDic[indateArray[curnum-1]])       
            sql="INSERT INTO month_return (ric, ts , monthly_return) VALUES ("+"'"+str(ric)+"','"+str(indate)+"','"+str(in_price)+"'"+")"
            #print sql
            try:
                cur.execute(sql)
                conn.commit()
            except Exception as e:
                errLog.write(sql+"\n"+str(e)+"\n")
                conn.rollback()
    if ttlnumber%1000==0:
        print str(time.clock())+"finishing handing"+str(ttlnumber)+'\n'
#print month_priceDic
#print indateArray

conn.close()
errLog.close()
fo.close()
