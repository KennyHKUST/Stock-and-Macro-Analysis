import MySQLdb

conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="",db="sys")
cur=conn.cursor()
filepath="/Users/lixinran/Documents/LiXi/testcsv.csv"
a = 0.89
print type(a) is float
b = "%.5f" % round(a,5)
print b
c = None

# with open(filepath,"rb") as csvfile:
# 	spamreader=csv.reader(csvfile, delimiter=",",quotechar='"')
# 	number=0
# 	for row in spamreader:
# 		number+=1
# 		Ric=row[0]
# 		Latest_Price=row[1]
# 		Latest_Volume=row[2]
# 		Market_Cap=row[3]
# 		sql="INSERT INTO VALUES (number,Ric,Latest_Price,Latest_Volume,Market_Cap)"
# 		try:
# 		    cur.execute(sql)
#         	conn.commit()
#         except:
#    			conn.rollback()
# 		continue
conn.close()

		# if return10180041 == "":
		# 	return10180041 = "NULL"
		# 	print return10180041
		#sql = "'" + variables['name'] + "','" + ric + ",'" + country + "','" + marketCap + "','" + adv + "','"
		#sql += sector + "','" + industry + "','" + return10180041 + "','" + return10181119 + "','"

		#sql="INSERT INTO sys.Interval_Return_430 VALUES ("+"name, ric, country, ,Market_Cap)"
		#print sql
		# try:
		#     cur.execute(sql)
  #       	conn.commit()
  #       except:
  #  			conn.rollback()
		# continue
#conn.close()

		# name = row[0]
		# ric = row[1]
		# country = row[2]
		# marketCap = row[3]
		# adv = row[4]
		# sector = row[5]
		# industry=row[6]
		# return10180041 = row[7]
		# return10181119 = row[8]
		# return10180306 = row[9]
		# return11190306 = row[10]
		# return10181231 = row[11]
		# return03061231 = row[12]
		# return11180470 = row[13]
