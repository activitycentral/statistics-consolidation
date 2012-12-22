from __future__ import print_function
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime

class DB_Stats:
	TABLES={}
	
	TABLES['Usages'] = (
		"CREATE TABLE `Usages` (" 
		"	`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," 
		"	`user_id` INTEGER NOT NULL," 
		"	`resource_id` INTEGER NOT NULL," 
		"	`start_date` TIMESTAMP NOT NULL," 
		"	`data_type` CHAR (30) NOT NULL,"
		"	`data` INTEGER NOT NULL,"
		"	PRIMARY KEY (`start_date`,`resource_id`, `data_type`)"
		"	)")

	TABLES['Resources'] = (
		"CREATE TABLE Resources ("
		"	`id` INTEGER auto_increment unique,"	
		"	`name` CHAR(250),"
		"	PRIMARY KEY (name)"
		"	)")

	TABLES['Users'] = (
		"CREATE TABLE Users("
		"	`id` INTEGER auto_increment unique,"	
		"	`machine_sn` CHAR(80),"
		"	`age` INTEGER NOT NULL,"
		"	`school` CHAR(80),"
		"	`sw_version` CHAR (80),"
		"	`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
		"	PRIMARY KEY (machine_sn)"
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
			print("Failed creating database: {}".format(err))
			exit(1)
	
	def create_tables(self, cursor):
		for name, ddl in self.TABLES.iteritems():
			try:
				print("Creating table {}: ".format(name), end='')
				cursor.execute(ddl)
		    	except mysql.connector.Error as err:
				if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
			    		print("already exists.")
				else:
			    		print("Error: {}".format(err))
		    	else:
				print("OK")

	def create (self):
		self.cnx = mysql.connector.connect(user='root', password='gustavo')
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
				print("Error: {}".format(err))
				exit(1)
		cursor.close()
	
			
		
	def close (self):
		self.cnx.close()



	def store_activity_uptime(self, rrd, data):
		cursor = self.cnx.cursor()
		insert = ("INSERT INTO Usages " 
				"(user_id, "
				"resource_id, "
				"start_date, "
				"data_type, "
				"data) "
				"VALUES (%s, %s, %s, %s ,%s) ")
		print ("store_activity_uptime: {}".format(self.get_resource_id(data['resource_name'])))
		info = ('none', self.get_resource_id(data['resource_name']), datetime.fromtimestamp(float(data['date_start'])), 'uptime', data['data'])	
		
		try:
			cursor.execute(insert, info)
			if self.update_last_record(rrd.get_last_record()) == 0:	
				self.cnx.commit()

		except mysql.connector.Error as err:
			print("Fail INSERT: {}".format(err))
		
		cursor.close()

	def get_resource_id(self, resource_name):
		cursor = self.cnx.cursor()
		op = ("SELECT id FROM Resources WHERE name = %s")
		params = (resource_name,)
		try:	
			cursor.execute(op, params)
			result = cursor.fetchone()
			if result != None:
				print ("result is not None: {}".format(result))
				return result[0]
			else:
				print ("result is None")
				insert = ("INSERT INTO Resources (name) VALUES (%s)")
				info = (resource_name, )
				cursor.execute(insert, info)
				self.cnx.commit()
				""" TODO: return ID instead call recursevily """
				self.get_resource_id(resource_name)
		except mysql.connector.Error as err:
			print("Fail SELCT: {}".format(err))

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
				print("will update ..")
				op = ("UPDATE Runs SET last_ts = %s")
                		cursor.execute(op, params)
				self.cnx.commit()
				print("update ok")
			else:
				print("will insert ..")
				op = ("INSERT INTO Runs VALUES(%s)")
				cursor.execute(op, params)
				self.cnx.commit()
				print("insert ok")

                except mysql.connector.Error as err:
                        print("Fail {}: {}".format(cursor.statement, err))
			res = -1

                cursor.close()
		return res
	def get_last_record (self):
		cursor = self.cnx.cursor()
                op = ("SELECT UNIX_TIMESTAMP ((SELECT last_ts FROM Runs))")
                try:
                        cursor.execute(op)
			result = cursor.fetchone()
			if result != None:
				print ("last rec: {}".format(result[0]))
                                return result[0]
                        else:
                                print ("result is None")
				return 0
                except mysql.connector.Error as err:
                        print("Fail SELELCT: {}".format(err))
                cursor.close()
