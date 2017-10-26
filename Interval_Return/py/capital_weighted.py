import MySQLdb, os, time, csv
from insert_query_MySQL import *
from data_scheme import *
def capital_weighted(cur_country, cur_field):
	print "Start calculating Capital Weighted of " + cur_country + " on " + cur_field
	for each in returns:#truncate the data in the dict
		returns[each] = []
	db = Database()
	txtname = cur_country + "_" + cur_field + "_capitalWeighted.txt"
	intervalLog = open(txtname, "w")
	#get all sectors first to compute sector/ industry return

	sector_query = "SELECT distinct (" + cur_field + ") FROM sys.Interval_Return_430 where country = '" + cur_country + "';"
	sector_result = db.query(sector_query)
	for sector in sector_result:
		#print sector[0]
		returns[cur_field].append(sector[0])
		#print returns[cur_field]
		#calculate the total market Cap of each sector/ industry and each interval
		for interval in intervals:
			Market_Cap_query = "SELECT sum(Market_Cap) FROM " + db_name + "." + table
			whole_query = " where " + cur_field + " = '" + sector[0] + "' and Country = '" 
			whole_query += cur_country + "' and " + interval + " is not NULL"
			Market_Cap_query += whole_query 
			Market_Cap_result = db.query(Market_Cap_query)
			#print sector[0] + ' ' + interval
			#print Market_Cap_result[0][0]
			return_query = "SELECT Market_Cap, " + interval + " FROM " + db_name + "." + table
			return_query += whole_query
			#print return_query
			return_results = db.query(return_query)
			total_weighted_return = 0
			for return_result in return_results:
				weighted_return = return_result[0] / Market_Cap_result[0][0]
				weighted_return *= return_result[1]
				total_weighted_return += weighted_return
			returns[interval].append(total_weighted_return)
	#print returns[cur_field]
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
	#print returns[cur_field]

