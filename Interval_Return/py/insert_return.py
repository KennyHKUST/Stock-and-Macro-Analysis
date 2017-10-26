import MySQLdb, os, time, csv
from insert_query_MySQL import *
from data_scheme import *
#conn = MySQLdb.connect(host="127.0.0.1",user="root",passwd="",db="sys")
#cur=conn.cursor()
db = Database()

with open(filepath,"rb") as csvfile:
	spamreader = csv.reader(csvfile, delimiter=",",quotechar='"')
	number = 0

	for row in spamreader:
		if number == 0:
			number += 1
			continue
		if number == 3:
			break
		#print row
		variables = {}
		number += 1
		count = 0
		for each in array:
			variables[each] = row[count]
			count += 1
		# variables = {
		# 	'name' : row[0],
		# 	'ric' : row[1],
		# 	'country' : row[2],
		# 	'marketCap' : row[3],
		# 	'adv' : row[4],
		# 	'sector' : row[5], 
		# 	'industry' : row[6],
		# 	'return10180041' : row[7],
		# 	'return10181119' : row[8],
		# 	'return10180306' : row[9],
		# 	'return11190306' : row[10],
		# 	'return10181231' : row[11],
		# 	'return03061231' : row[12],
		# 	'return11180470' : row[13],
		# 	'return12010470' : row[14]
		# }
		try:# if adv == 0, take it as no data 
			if float(variables['adv']) == 0:
				variables['adv'] = ''
		except:
			continue
		try: #avoid overflow of the SQL data
			p = float(variables['marketCap'])
			#change the precision to five decimals
			variables['marketCap'] = "%.5f" % round(p,5)
		except:
			continue
		#print variables['marketCap']
		index = variables['name'].find('\'')
		while index != -1: #totally eliminate the quote in the company name,
		#as quote is not accepted in inserting
			if index != -1:
				#print variables['name']
				newName = list(variables['name'])
				newName[index] = ''
				newString = ''.join(newName)
				index = newString.find('\'')
				variables['name'] = newString

		sql = "INSERT INTO " + db_name + '.' + table + " VALUES ("
		count = 0
		for column in array:
			if variables[column] != "" :
				sql += "'" + variables[column] + "'"
			else: #if NULL, then no quotation around
				sql += "NULL"
				variables[column] = "NULL"
			if count != len(array)-1: 
				sql += ","
			count += 1
		sql += ")"
		#print sql
		db.insert(sql) #execute sql statement
		#print 
	print number
	print "insert into MySQL finished"