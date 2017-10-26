#from cal_interval_returns import *

array = ['name', 'ric', 'country', 'marketCap', 'adv', 'sector', 'industry', 'return10180041', 'return10181119', 'return10180306', 'return11190306', 'return10181231', 'return03061231', 'return11180470', 'return12010470']

intervals = ["991018_000401", 
"991018_991119", 
"991018_000306", 
"991119_000306", 
"991018_001231", 
"000306_001231", 
"161118_170430",
"161201_170430"]

#either declare one variable or an array
#You can iterate the array to get all data once for all
#has to follow the country in MySQL
#cur_country = "Hong_Kong" 
table = "Interval_Return_430"
db_name = 'sys'
filepath = "Japan430.csv"
cur_field = 'industry' # set sector or industry 
cur_country_array = ["Japan", "United_States", "Europe", "Hong_Kong"]

returns = {
	cur_field:[],
	"991018_000401": [], 
	"991018_991119": [], 
	"991018_000306": [], 
	"991119_000306": [], 
	"991018_001231": [], 
	"000306_001231": [], 
	"161118_170430": [],
	"161201_170430": []

}


