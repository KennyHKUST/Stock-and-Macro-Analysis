import MySQLdb


class Database():

    host = '127.0.0.1'
    user = 'root'
    password = ''
    db = 'sys'

    def __init__(self):
        self.connection = MySQLdb.connect(self.host, self.user, self.password, self.db)
        self.cursor = self.connection.cursor()

    def insert(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
			print query + '\n' + str(e)
            #self.connection.rollback()

    def truncate(self, table):
    	#query = "TRUNCATE `sys`.`Interval_Return_430`;"
    	query = "TRUNCATE `" + self.db + "`.`" + table +"`;"
    	print query
    	self.insert(query)
    def query(self, query):
        #cursor = self.connection.cursor(MySQLdb.cursors.DictCursor )
        cursor = self.cursor
        cursor.execute(query)

        return cursor.fetchall()
    # def query_array(query):
    #     cursor = 
    def __del__(self):
        self.connection.close()
db = Database()
#db.truncate(table)



