from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime

class DB_Stats:
	TABLES={}
	
	TABLES['Usages'] = (
		"CREATE TABLE `Usages` (" 
		"	`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," 
		"	`user_hash` CHAR(40) NOT NULL," 
		"	`resource_name` CHAR(80)," 
		"	`start_date` TIMESTAMP NOT NULL," 
		"	`data_type` CHAR (30) NOT NULL,"
		"	`data` INTEGER NOT NULL,"
		"	PRIMARY KEY (`user_hash`,`start_date`,`resource_name`, `data_type`)"
		"	)")

	TABLES['Resources'] = (
		"CREATE TABLE Resources ("
		"	`name` CHAR(250),"
		"	PRIMARY KEY (name)"
		"	)")

	TABLES['Users'] = (
		"CREATE TABLE Users("
                "       `hash` CHAR (40) NOT NULL,"	
		"       `uuid` CHAR (32) NOT NULL,"
		"	`machine_sn` CHAR(80),"
		"	`age` INTEGER NOT NULL,"
		"	`school` CHAR(80),"
		"	`sw_version` CHAR (80),"
		"	`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
		"	PRIMARY KEY (hash)"
		"	)")

	TABLES['Runs'] = (
		"CREATE TABLE Runs("
		"	`last_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP "	
		")")



	def __init__(self,  db_name, user, password):
		self.db_name  = db_name
		self.user = user
		self.password = password
		

	def create_database(self, cursor):
		try:
			cursor.execute(
			"CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.db_name))
		except mysql.connector.Error as err:
			raise Exception ("Failed creating database: {}".format(err))
	
	def create_tables(self, cursor):
		for name, ddl in self.TABLES.iteritems():
			try:
				print("Creating table {}: ".format(name), end='')
				cursor.execute(ddl)
		    	except mysql.connector.Error as err:
				if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
			    		print("already exists.")
				else:
			    		raise Exception ("Error: {}".format(err))
		    	else:
				print("OK")

	def create (self):
		self.cnx = mysql.connector.connect(user=self.user, password=self.password)
		cursor = self.cnx.cursor()
		"""Try connect to db """
		try:
		    	self.cnx.database = self.db_name
			print("DB ["+self.db_name+"] created already, will try create tables:" )
			self.create_tables(cursor)
		except mysql.connector.Error as err:
			"""If db not exist, then create"""
		    	if err.errno == errorcode.ER_BAD_DB_ERROR:
				self.create_database(cursor)
		    		self.cnx.database = self.db_name
				self.create_tables(cursor)
		    	else:
				raise Exception ("Error: {}".format(err))
		cursor.close()
	
			
		
	def close (self):
		self.cnx.close()



	def store_activity_uptime(self, rrd):
		
		self.store_resource(rrd.get_name())
		self.store_user(rrd)
		
		cursor = self.cnx.cursor()
		insert = ("INSERT INTO Usages " 
				"(user_hash, "
				"resource_name, "
				"start_date, "
				"data_type, "
				"data) "
				"VALUES (%s, %s, %s, %s ,%s) ")

		for d in rrd.get_uptime_by_interval():
			info = (rrd.get_user_hash(), rrd.get_name() , datetime.fromtimestamp(float(d[0])), 'uptime', d[1])	
			try:
				cursor.execute(insert, info)
				if self.update_last_record(rrd.get_date_last_record()) == 0:	
					self.cnx.commit()

			except mysql.connector.Error as err:
                        	print("Fail {}: {}".format(cursor.statement, err))
		cursor.close()
	

	def store_resource(self, resource_name):
		cursor = self.cnx.cursor()
		op = ("SELECT name FROM Resources WHERE name = %s")
		params = (resource_name,)
		try:	
			cursor.execute(op, params)
			result = cursor.fetchone()
			if result != None:
				print("Resource {} already in db".format(resource_name))
			else:
				insert = ("INSERT INTO Resources (name) VALUES (%s)")
				info = (resource_name, )
				cursor.execute(insert, info)
				self.cnx.commit()
		except mysql.connector.Error as err:
                        print("Fail {}: {}".format(cursor.statement, err))

		cursor.close()

	def store_user (self, rrd):
		cursor = self.cnx.cursor()
                op = ("SELECT hash FROM Users WHERE hash = %s")
                params = (rrd.get_user_hash(), )
                try:    
                        cursor.execute(op, params)
                        result = cursor.fetchone()
                        if result != None:
                                print("User {} already in db".format(rrd.user_hash))
                        else:
				"""FIXME change hardcoded values """
                                insert = ("INSERT INTO Users (hash, uuid, machine_sn, age, school, sw_version) VALUES (%s, %s, %s, %s, %s, %s)")
                		params = (rrd.get_user_hash(), rrd.get_uuid(), "unk_machine_sn", 0, "unk_escuela", "1.0.0")
                                cursor.execute(insert, params)
                                self.cnx.commit()
                except mysql.connector.Error as err:
                        print("Fail {}: {}".format(cursor.statement, err))

                cursor.close()



	def update_last_record (self, ts):
		cursor = self.cnx.cursor()
		res = 0
                op = ("SELECT * FROM Runs")
                params = (datetime.fromtimestamp(float(ts)),)
                try:
			cursor.execute(op)
			result = cursor.fetchone()

			if result != None:
				op = ("UPDATE Runs SET last_ts = %s")
                		cursor.execute(op, params)
				self.cnx.commit()
			else:
				op = ("INSERT INTO Runs VALUES(%s)")
				cursor.execute(op, params)
				self.cnx.commit()

                except mysql.connector.Error as err:
                        print("Fail {}: {}".format(cursor.statement, err))
			res = -1

                cursor.close()
		return res

	def get_date_last_record (self):
		cursor = self.cnx.cursor()
                op = ("SELECT UNIX_TIMESTAMP ((SELECT last_ts FROM Runs))")
                try:
                        cursor.execute(op)
			result = cursor.fetchone()
			if result != None:
				print ("last record: {}".format(result[0]))
                                return result[0]
                        else:
                                print ("Last date record is None")
				return 0
                except mysql.connector.Error as err:
                        print("Fail {}: {}".format(cursor.statement, err))
                cursor.close()
