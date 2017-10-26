from capital_weighted import *
from equally_weighted import *
from data_scheme import *
#running for single country
# capital_weighted(cur_country, cur_field)
# equally_weighted(cur_country, cur_field)

#running for all countries
for cur_country in cur_country_array:
	equally_weighted(cur_country, cur_field)
	capital_weighted(cur_country, cur_field)
