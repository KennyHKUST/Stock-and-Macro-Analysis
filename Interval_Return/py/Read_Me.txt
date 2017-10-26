/* Remarks for MySQL Insertion: */

If adv = 0, ‘’(nothing) or NULL, NULL will be inserted on adv Column.

If Market Cap is not a number, the row will be ignored.

All valid Market Cap will be rounded to five decimal places.

If there exists quotes in company name, they will be first deleted from the name and then inserted into the database.

Modify the table name, db name and file path in data_scheme.py for correct insertion

After everything is set up, in shell command line, on the shell, run
$ python insert_returns.py 

/* Remarks for Sector/Industry Interval Return Calculation: */

Edit the ‘array’ , ‘intervals’, ’returns’ to fit the intervals

You may change cur_field to ‘sector’ get sector return or ‘industry’ to get industry return

You may either use single country or country array to get the return for one country or all countries. To do this, you need to modify both the data_scheme.py and cal_interval_returns.py

After everything is set up, in shell command line, on the shell, run 
$ python cal_interval_returns.py 


~Contact Kenny Li (xlica@ust.hk / (+852) 69155002) for any issues~
