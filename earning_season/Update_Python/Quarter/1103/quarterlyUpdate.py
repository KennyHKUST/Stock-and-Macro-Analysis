import MySQLdb,time,os,csv,datetime #import libraries
from datetime import datetime
from dbsettings import *
#connect to mysql using variables defined in dbsettings
conn = MySQLdb.connect(host=RAW_HOST,user=RAW_USER,passwd=RAW_PASS,db=DB_NAME)
cur=conn.cursor() #obtain the cursor to execute mysql codes 
fo=open("null.log","w") #document rics whose accounting reports are unavailable
foappend=open("append.log","w") #document rics whose data is appended this time
foupdate=open("update.log","w") #document rics whose data is updated(same date)
errLog = open("newerr.log","w") #document sql execution error
rootdir="/Users/lixinran/Documents/LiXi/earning_season/Update_file/Update_quarter/1103"
filefolder=os.listdir(rootdir) #obtain all files in the folder directory(for annual/semi/quarter, only one csv)
for file in filefolder: #iterate files in the folder
    if file[len(file)-1] !="v": #ignore non-CSV file
        continue
    filepath=os.path.join(rootdir,file) #obtain the file path
    print filepath
    with open(filepath,"rU") as csvfile: #loop to open CSV files in the folder
        spamreader=csv.reader(csvfile, delimiter=",",quotechar='"') #obtain the content of csv file, saving by rows
        number=0 #count how many rows have been read
        csvColData={} #initiate two dictionaries
        sqlColData={}
        for row in spamreader:
            number+=1
            if number%1000==0:
                print str(number)+'\t'+str(datetime.now())
            if number==1: 
                continue #ignore the first row
            ric=row[0] #obtain the ric from the first cell in the row, starting from the second row
            if row[2]=="": #ignore and document the ric whose accounting data is unavailable
                fo.write(ric+'\n')
                continue 
            date=row[2] #obtain the fiscal end date from the third cell in the row
            #print ric
            #The follwing two lines change the format of the date from d/m/Y to Y-m-d
            #i.e 31/12/2015 to 2015-12-31 so that it can be inserted into mysql
            tmpTS = time.strptime(date,"%m/%d/%Y")
            indate = time.strftime("%Y-%m-%d",tmpTS)
            #Obtain the unix timestamp from fiscal end date for comparison
            timeStamp = int(time.mktime(tmpTS))
            sqlin="INSERT INTO trweb.tr_report_quarter (ric, ts"            
            for i in range(4,31): #iterate the columns in the csv from fy to SGA Expense
                sqlin+=","+colDict[i-3] #add the targeted inserted column into mysql code 
            sqlin+=")VALUES ("+"'"+str(ric)+"','"+str(indate)+"'"
            #print sqlin
            for i in range(3,30): #iterate the columns in the csv from revenue to SGA Expense 
                #print i
                if row[i]=="": #insert NULL if the data item is empty (data unavailable)
                    data="NULL"
                    sqlin+=","+str(data) #no quotation when inserting NULL into mysql table
                else:
                    try:                    
                        data=float(row[i]) #the updated data contains integers in most of columns.
                        #need to be changed when retrieved data is not integer later
                    except:
                        data=row[i] #negative number cannot be converted to integer/need to be changed when retrived data is not integer later
                    sqlin+=","+"'"+str(data)+"'" #with two single quotations when not inserting NULL 
                csvColData[colDict[i-2]]=data #push the data of each item obtained from csv into the dictionary
                #print sqlin
            sqlin+=")" #end the sql code
            #print "csvColData:"
            #print csvColData
            #exit(0)
            #the following line select the stock latest fiscal end date in unix time format
            sql="SELECT unix_timestamp(max(ts)) FROM trweb.tr_report_quarter where ric='"+str(ric)+"'"
            try: #get the latest unix timestamp by executing the sql code
                cur.execute(sql)
                results=cur.fetchall()
                for row in results:
                    ts=row[0]
            except Exception as e: #document the error if the execution is failed
                errLog.write(sql+"\n"+str(e)+"\n")
                conn.rollback()
            #print ts
            #print timeStamp
            #exit(0)
            if timeStamp>ts: #if the timestamp in csv is larger(more updated)than 
            #the one stored in database before, insert the updated data 
                #print sqlin

                #for i in range(3,32):
                try:
                    cur.execute(sqlin)
                    conn.commit()
                    foappend.write(ric+", Updated at "+indate+'\n')

                except Exception as e: #document the sql execution error
                    errLog.write(sqlin+"\n"+str(e)+"\n")
                    conn.rollback()
            elif timeStamp==ts: #update by item
                sqlcheck="SELECT * FROM trweb.tr_report_quarter where ric='"+str(ric)+"' and ts='"+str(indate)+"'" 
                #print sqlcheck
                #exit(0)
                try:
                    cur.execute(sqlcheck)
                    results=cur.fetchall()
                    for row in results:
                        for i in range(3,32):
                            if i-2==26 or i-2==27:
                                continue
                            #print i-2
                            sqlColData[sqlcolDict[i-2]]=row[i] #push the data stored in sql into dictionary
                            if sqlColData[sqlcolDict[i-2]]==None: #convert None into NULL
                                sqlColData[sqlcolDict[i-2]]="NULL"
                            else:
                                sqlColData[sqlcolDict[i-2]]=float(row[i]) #convert the float into integer
                                #need to be changed when retrieved data is no longer integer later
                            if sqlColData[sqlcolDict[i-2]]=="NULL" or csvColData[sqlcolDict[i-2]]=="NULL":
                                if sqlColData[sqlcolDict[i-2]]=="NULL" and csvColData[sqlcolDict[i-2]]=="NULL":
                                    continue
                            elif float(sqlColData[sqlcolDict[i-2]])-float(csvColData[sqlcolDict[i-2]])<1: #by item, if the data in csv is different from the one in mysql
                                continue
                            print sqlColData[sqlcolDict[i-2]]
                            print csvColData[sqlcolDict[i-2]]
                            #print "bad"
                            sqlupdate="Update trweb.tr_report_quarter set " #construct sql code
                            if csvColData[sqlcolDict[i-2]]=="NULL": #update NULL (empty data)to the particular item (no quotation)
                                sqlupdate+=sqlcolDict[i-2]+"="+ str(csvColData[sqlcolDict[i-2]]) +" where ric='" +str(ric)+"' and ts='"+str(indate)+"'"
                            else: #update non-NULL data to the particular item(with two single quotations)
                                sqlupdate+=sqlcolDict[i-2]+"='"+str(csvColData[sqlcolDict[i-2]])+"'where ric='" +str(ric)+"' and ts='"+str(indate)+"'"
                            print sqlupdate
                            try: 
                                cur.execute(sqlupdate)
                                conn.commit()
                                foupdate.write(ric+'\n')
                            except Exception as e:
                                errLog.write(sqlupdate+"\n"+str(e)+"\n")
                                conn.rollback()
                except Exception as e:
                    errLog.write(sqlcheck+"\n"+str(e)+"\n")
                    conn.rollback()
            continue #ignore rest of the content, direct to next row

#close the following documents    
conn.close()
errLog.close()
fo.close()
foappend.close()
foupdate.close()
