import MySQLdb, os, time, csv
from insert_query_MySQL import *
from data_scheme import *
#The output will be based on the variable declared in data_scheme
def equally_weighted(cur_country, cur_field):
	print "Start calculating Equally Weighted of " + cur_country + " on " + cur_field
	for each in returns:#truncate the data in the dict
		returns[each] = []
	db = Database()#initalize a database object
	txtname = cur_country + "_" + cur_field + "_equallyWeighted.txt"
	intervalLog = open(txtname, "w")
	SumCountsql = ""
	count = 0
	for interval in intervals:
		count += 1
		SumCountsql += "sum(" + interval + ")/count(" + interval + ")"
		if count != len(intervals):
			SumCountsql += ', '

	sql = "SELECT " + cur_field + " , "
	sql += SumCountsql
	sql += " FROM " + db_name + '.' + table +" where Country = '"+ cur_country +"' group by " + cur_field + ";"

	result = db.query(sql);#execute sql and store the output in the result

	for row in result:#iterate each row in the result
		i = 1
		returns[cur_field].append(row[0])
		for interval in intervals:
			if row[i] == None:
				returns[interval].append(0)
			else:
				returns[interval].append(row[i])#insert each column into the dict
			i += 1

	# print returns
	# exit(0)
	intervalLog.write('; ')
	for sector_industry in returns[cur_field]:
		intervalLog.write(sector_industry + '; ')
	intervalLog.write('\n')

	for interval in intervals:
		intervalLog.write(interval + "; ")
		for element in returns[interval]:
			intervalLog.write(str(element / 100) + '; ')
		intervalLog.write('\n')

	intervalLog.close()
	print txtname + " written completed"





