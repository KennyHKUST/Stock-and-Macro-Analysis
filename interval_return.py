import MySQLdb,time,os,csv
interval=['200931_201011',
'201021_201051',
'201051_201071',
'201071_201151',
'201091_201151',
'201171_2011101',
'2011101_201241',
'2011111_201531',
'201251_201261',
'201261_2012101',
'201581_201591',
'201621_201691',
'2015101_2015111',
'201581_201621',
'2016119_20161125']

conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="yang83546852A",db="trweb")
cur=conn.cursor()
intervalLog = open("USvalue_interval.txt","w")
sqlmkt_cap="SELECT country_of_exchange, new_master_id.sector, sum(market_cap) FROM trweb.new_master_id right join trweb.us_interval_return_3 on trweb.us_interval_return_3.ric=new_master_id.ric where country_of_exchange='United States' group by country_of_exchange, sector"
#returnArray=[]
intervalLog.write('; ')
for subinterval in interval:
    intervalLog.write(subinterval+'; ')
try:
    cur.execute(sqlmkt_cap)
    results=cur.fetchall()
    for row in results:
        sector=row[1]
        mkt_cap=row[2]
        print sector
        print mkt_cap
        intervalLog.write('\n'+sector)
        for i in interval:
            sqlget="SELECT sum("+str(i)+"*market_cap/"+str(mkt_cap)+") FROM trweb.us_interval_return_3 left join new_master_id on us_interval_return_3.ric=new_master_id.ric where country_of_exchange='United States' and new_master_id.sector='"+str(sector)+"'"
            try:
                cur.execute(sqlget)
                VWR=cur.fetchall()
                for valueR in VWR:
                    R=float(valueR[0])
                    print R
                    intervalLog.write(str(R)+' ;')
            except Exception as e:
                print sqlget+'\n'+str(e)
                conn.rollback()

except Exception as e:
    print sqlmkt_cap+'\n'+str(e)
    conn.rollback()

print interval[0]

intervalEqualLog = open("USequal_interval.txt","w")
intervalEqualLog.write('; ')
sqlgetsector="SELECT  new_master_id.sector FROM trweb.us_interval_return_3 left join trweb.new_master_id on us_interval_return_3.ric=new_master_id.ric  where country_of_exchange='United States' group by  new_master_id.sector"
try:
    cur.execute(sqlgetsector)
    results=cur.fetchall()
    for row in results:
        sector=row[0]
        intervalEqualLog.write(sector+'; ')
except Exception as e:
    print str(sqlgetsector)+'\n'+str(e)
    conn.rollback()
for z in interval:
    intervalEqualLog.write('\n'+z+'; ')
    sqlValue="SELECT  new_master_id.sector, avg("+str(z)+") FROM trweb.us_interval_return_3 left join trweb.new_master_id on us_interval_return_3.ric=new_master_id.ric  where country_of_exchange='United States' group by  new_master_id.sector"
    print sqlValue
    try:
        cur.execute(sqlValue)
        results=cur.fetchall()
        for row in results:
            avg=float(row[1])
            intervalEqualLog.write(str(avg)+'; ')
    except Exception as e:
        print sqlValue+'\n'+str(e)
        conn.rollback()
