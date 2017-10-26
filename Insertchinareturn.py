import MySQLdb,time,os,csv

insertnumber=0
conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
fo=open("null.log","w")
foappend=open("append.log","w")
errLog = open("newerr.log","w")
rootdir="/Users/lixinran/Documents/LiXi/Return"
filefolder=os.listdir(rootdir)
for file in filefolder:
    if file[len(file)-1] !="v":
        continue
    filepath=os.path.join(rootdir,file)
    #print filepath
    with open(filepath,"rb") as csvfile:
        insertnumber+=1
        spamreader=csv.reader(csvfile, delimiter=",",quotechar='"')
        number=0
        numCol=0
        for row in spamreader:
            number+=1
            if number==1:
                ric=row[0]
                if ric=="RIC": #avoid error
                    fo.write(filepath+"\n")
                    break
                continue
            
            #sql="SELECT unix_timestamp(max(ts)) FROM trweb.tr_report_annual where ric='"+str(ric)+"'"
            if row[2]=="":
                break
            date=row[2]
            One_month_return=row[3]
            if One_month_return=="":
                continue
            if date=="":
                break


            print ric
            #tmpTS = time.strptime(date,"%m/%d/%Y")
            tmpTS = time.strptime(date,"%m/%d/%Y")
            indate = time.strftime("%Y-%m-%d",tmpTS)

            sql="INSERT INTO China_Return (Ric, ts , One_month_return) VALUES ("+"'"+str(ric)+"','"+str(indate)+"','"+str(One_month_return)+"'"+")"
            print sql
            try:
                cur.execute(sql)
                conn.commit()
                #foappend.write(ric+'\n')
            except Exception as e:
                errLog.write(sql+"\n"+str(e)+"\n")
                conn.rollback()
            continue
            
    if insertnumber%1000==0:
        print str(time.clock())+"finishing handing"+str(insertnumber)+'\n'

            
conn.close()
errLog.close()
fo.close()

