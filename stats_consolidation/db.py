import logging
from datetime import datetime

import mysql.connector
from mysql.connector import errorcode

from stats_consolidation.rrd_files import *

log = logging.getLogger("stats-consolidation")

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
			"CREATE DATABASE {0} DEFAULT CHARACTER SET 'utf8'".format(self.db_name))
		except mysql.connector.Error as err:
			raise Exception ("Failed creating database: {0}".format(err))
	
	def create_tables(self, cursor):
		for name, ddl in self.TABLES.iteritems():
			try:
				log.info('Creating table %s:', name)
				cursor.execute(ddl)
			except mysql.connector.Error as err:
				if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
					log.warning('Table %s already exists.', name)
				else:
					raise Exception ("Error: {0}".format(err))
			else:
				log.info('Table %s crated', name)

	def create (self):
		self.cnx = mysql.connector.connect(user=self.user, password=self.password)
		cursor = self.cnx.cursor()
		"""Try connect to db """
		try:
			self.cnx.database = self.db_name
			log.info('Data Base %s already created, will create tables',  self.db_name)
			self.create_tables(cursor)
		except mysql.connector.Error as err:
			"""If db not exist, then create"""
			if err.errno == errorcode.ER_BAD_DB_ERROR:
				self.create_database(cursor)
				self.cnx.database = self.db_name
				self.create_tables(cursor)
			else:
				raise Exception ("Error: {0}".format(err))
		cursor.close()
	
	
			
		
	def close (self):
		self.cnx.close()
	
	def connect (self):
		try:
			self.cnx = mysql.connector.connect(user=self.user, password=self.password)
			cursor = self.cnx.cursor()
			self.cnx.database = self.db_name
			cursor.close()
		except mysql.connector.Error as err:
			raise Exception ("Error: {0}".format(err))


#=========================================================================================================
#				Q U E R I E S   S A V E   M E T H O D S
#=========================================================================================================
	
	def store_activity_uptime (self, rrd):
		self.store_activity_time (rrd, 'uptime')

	def store_activity_focus_time (self, rrd):
		self.store_activity_time(rrd, 'active')

	

	def store_activity_time(self, rrd, data_type):
		
		self.store_resource(rrd.get_name())
		self.store_user(rrd)
		
		cursor = self.cnx.cursor()
		select = ("SELECT * FROM Usages WHERE "
				"user_hash = %s AND "
				"resource_name = %s AND "
				"start_date = %s AND "
				"data_type = %s")
		update = ("UPDATE Usages SET data = %s WHERE "
				"user_hash = %s AND "
				"resource_name = %s AND "
				"start_date = %s AND "
				"data_type = %s")
		insert = ("INSERT INTO Usages " 
				"(user_hash, "
				"resource_name, "
				"start_date, "
				"data_type, "
				"data) "
				"VALUES (%s, %s, %s, %s ,%s) ")

		for d in rrd.get_last_value_by_interval(data_type):
			info_sel = (rrd.get_user_hash(), rrd.get_name() , datetime.fromtimestamp(float(d[0])), data_type)	
			try:
				"""Verify if this activity has an entry already at the same start_date"""
				cursor.execute (select, info_sel)
				result = cursor.fetchone()
	
				if result != None:
					log.info('Update %s \'%s\' entry for resource \'%s\' ', data_type, d[1], rrd.get_name())
					info_up = (d[1], rrd.get_user_hash(), rrd.get_name() , datetime.fromtimestamp(float(d[0])), data_type)	
					cursor.execute(update, info_up)
				else:
					log.info('New %s \'%s\' entry for resource \'%s\'', data_type, d[1], rrd.get_name())
					info_ins = (rrd.get_user_hash(), rrd.get_name() , datetime.fromtimestamp(float(d[0])), data_type, d[1])	
					cursor.execute(insert, info_ins)

				self.cnx.commit()

			except mysql.connector.Error as err:
							log.error('MySQL on store_activiy_time()%s: %s %s', data_type, cursor.statement, err)
		cursor.close()
	

	def store_resource(self, resource_name):
		cursor = self.cnx.cursor()
		op = ("SELECT name FROM Resources WHERE name = %s")
		params = (resource_name,)
		try:	
			cursor.execute(op, params)
			result = cursor.fetchone()
			if result != None:
				log.debug('Resource %s already present in DB', resource_name)
			else:
				insert = ("INSERT INTO Resources (name) VALUES (%s)")
				info = (resource_name, )
				cursor.execute(insert, info)
				self.cnx.commit()
				log.info('New Resource %s stored in DB', resource_name)
		except mysql.connector.Error as err:
			log.error('MySQL on store_resource:  %s %s', cursor.statement, err)

		cursor.close()

	def store_user (self, rrd):
		cursor = self.cnx.cursor()
		op = ("SELECT hash FROM Users WHERE hash = %s")
		params = (rrd.get_user_hash(), )
		try:    
			cursor.execute(op, params)
			result = cursor.fetchone()
			if result != None:
				log.debug('User %s already in DB', rrd.user_hash)
			else:
				insert = ("INSERT INTO Users (hash, uuid, machine_sn, age, school, sw_version) VALUES (%s, %s, %s, %s, %s, %s)")
				params = (rrd.get_user_hash(), rrd.get_uuid(), rrd.get_sn(), rrd.get_age(), rrd.get_school(), "1.0.0")
				cursor.execute(insert, params)
				self.cnx.commit()
				log.debug('New User %s stored in DB', rrd.user_hash)
		except mysql.connector.Error as err:
			log.error('MySQL on store_user %s %s', cursor.statement, err)

		cursor.close()



	def update_last_record (self):
		cursor = self.cnx.cursor()
		res = 0
		op = ("SELECT * FROM Runs")
		try:
			cursor.execute(op)
			result = cursor.fetchone()

			if result != None:
				op = ("UPDATE Runs SET last_ts = CURRENT_TIMESTAMP")
				cursor.execute(op)
				self.cnx.commit()
			else:
				op = ("INSERT INTO Runs VALUES(CURRENT_TIMESTAMP)")
				cursor.execute(op)
				self.cnx.commit()
			log.info("Save last record");
		except mysql.connector.Error as err:
			log.error('MySQL on update_last_record: %s %s', cursor.statement, err)
			res = -1

		cursor.close()
		return res

	def get_date_last_record (self):
		cursor = self.cnx.cursor()
		op = ("SELECT UNIX_TIMESTAMP ((SELECT last_ts FROM Runs))")
		try:
			cursor.execute(op)
			result = cursor.fetchone()
			if result != None and result[0] != None:
				log.info('Last record: %s', str(datetime.fromtimestamp (float (result[0]))))
				return result[0]
			else:
				log.info('Last date record is None')
				return 0
		except mysql.connector.Error as err:
			log.error('MySQL on get_date_last_record: %s %s',cursor.statement, err)
		except Exception as e:
			raise Exception ("get_date_last_record: {0}".format(e))
		cursor.close()





#=========================================================================================================
#				R E P O R T   M E T H O D S
#=========================================================================================================
	def rep_activity_time (self, start, end, activity, school=None):
		uptime_last=0
		activity_name=''
		focus = 0
		uptime = 0
		ts_start = self.date_to_ts(start)
		ts_end   = self.date_to_ts(end)
			
		cursor1 = self.cnx.cursor()
		cursor2 = self.cnx.cursor()
		try:
			if school != None:
				select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s)  AND (start_date > %s) AND (start_date < %s) AND (data_type = %s) AND (user_hash = %s)"
				
				log.debug('Activiy time by school: %s', school)
				""" Get user hash from a School"""
				cursor1.execute ("SELECT hash FROM Users WHERE school = %s", (school,))
				user_hashes = cursor1.fetchall()
				for user_hash in user_hashes:
					log.debug('user Hash: %s', user_hash[0])
					params_focus = (activity, ts_start, ts_end, 'active', user_hash[0])
					params_uptime = (activity, ts_start, ts_end, 'uptime', user_hash[0])
				
					cursor2.execute(select_usage, params_focus)
					focus = float (cursor2.fetchone()[0]) + focus
					cursor2.execute(select_usage, params_uptime)
					uptime = float (cursor2.fetchone()[0]) + uptime
				
			else:
				select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s)  AND (start_date > %s) AND (start_date < %s) AND (data_type = %s)"
				params_focus = (activity, ts_start, ts_end, 'active')
				params_uptime = (activity, ts_start, ts_end, 'uptime')
				cursor2.execute(select_usage, params_focus)
				focus = float(cursor2.fetchone()[0])
				cursor2.execute(select_usage, params_uptime)
				uptime = float(cursor2.fetchone()[0])
			
			log.debug('Times of (%s) from: %s -> %s: Uptime: %s, Focus: %s', activity, start, end, uptime, focus)
			
					
			cursor1.close()
			cursor2.close()
			return (uptime, focus)
		except mysql.connector.Error as err:
			log.error('MySQL on rep_activity_time %s', err)
		except Exception as e:
			log.error('MySQL on rep_activity_time : %s', e)
		return (None, None)



	def rep_get_activities (self, start, end, school=None, desktop='any'):
		res_list = list();
		cursor1 = self.cnx.cursor()
		cursor2 = self.cnx.cursor()
		cursor3 = self.cnx.cursor()
		
		if desktop == 'gnome':
			cursor2.execute("SELECT name FROM Resources WHERE name REGEXP 'application'")
		elif desktop == 'sugar':
			cursor2.execute("SELECT name FROM Resources WHERE name REGEXP 'activity'")
		else:	
			cursor2.execute("SELECT name FROM Resources")
		
		resources = cursor2.fetchall()
			 
		try:
			if school != None:
				log.debug('Most activiy used by school: %s', school)
				""" Get user hash from a School"""
				cursor1.execute ("SELECT hash FROM Users WHERE school = %s", (school,))
				user_hashes = cursor1.fetchall()
				""" Cursor for select resources from Uages table"""
				select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s) AND (start_date > %s) AND (start_date < %s) AND (data_type = 'active') AND (user_hash = %s)"
			else:
				log.debug('Most activiy used')
				""" Cursor for select resources from Uages table"""
				select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s) AND (start_date > %s) AND (start_date < %s) AND (data_type = 'active')" 
				
			

			ts_start = self.date_to_ts(start)
			ts_end   = self.date_to_ts(end)


			for resource in resources:
				log.debug('Resource: %s', resource[0])
				if self.is_an_activity (resource[0]):
					if school != None:
						for user_hash in user_hashes:
							log.debug('user Hash: %s', user_hash[0])
							cursor3.execute(select_usage, (resource[0], ts_start, ts_end, user_hash[0]))
							focus = cursor3.fetchone()[0]
							if focus == None: focus = 0

							log.debug('Focus time: %s', focus)
							res_list.append((resource[0], focus))
					else:
						cursor3.execute(select_usage, (resource[0], ts_start, ts_end))
						focus = cursor3.fetchone()[0]
						if focus == None: focus = 0
						log.debug('Focus time: %s', focus )
						res_list.append((resource[0], focus))
						
		except  mysql.connector.Error as err:
			log.error('MySQL on most_activity_used %s', err)
		except Exception as e:
			log.error('most_activity_used  Fail: %s', e)
		cursor1.close()
		cursor2.close()
		cursor3.close()
		log.debug ('Activities: %s', sorted(res_list, key=lambda x: x[1], reverse=True))
		return sorted(res_list, key=lambda x: x[1], reverse=True)		

					

	def rep_frequency_usage (self, start, end, school=None):
		cursor1 = self.cnx.cursor()
		cursor2 = self.cnx.cursor()
		user_hashes=()
		time = 0
		try:
			ts_start = self.date_to_ts(start)
			ts_end   = self.date_to_ts(end)
			
			if school != None:
				log.debug('Frequency usage by school: %s', school)
				""" Get user hash from a School"""
				cursor1.execute ("SELECT hash FROM Users WHERE school = %s", (school,))
				user_hashes = cursor1.fetchall()
			
				for user_hash in user_hashes:
					cursor2.execute("SELECT SUM(data) FROM Usages WHERE (resource_name = 'system') AND (start_date > %s) AND (start_date < %s) AND (data_type = 'uptime') AND (user_hash = %s)", (ts_start, ts_end, user_hash[0]))
					res = cursor2.fetchone()
					if res != None and res[0] != None:
						time = float (res[0]) + time 
			else:
				log.debug('Frequency usage')
				cursor1.execute ("SELECT hash FROM Users")
				user_hashes = cursor1.fetchall()
				cursor2.execute("SELECT SUM(data) FROM Usages WHERE (resource_name = 'system') AND (start_date > %s) AND (start_date < %s) AND (data_type = 'uptime')", (ts_start, ts_end))
				time = cursor2.fetchone()[0]
		
			return (time, len(user_hashes))
				

		except mysql.connector.Error as err:
			log.error("MySQL on %s: %s", cursor.statement, err)
		cursor1.close()	
		cursor2.close()	


	def rep_update_school(self, machine_sn, school):
		cursor = self.cnx.cursor()
		try:
			log.debug("Set school name: %s to user with machine_sn: %s", school, machine_sn)
			cursor.execute ("UPDATE Users SET school = %s WHERE machine_sn = %s", (school, machine_sn))
		except mysql.connector.Error as err:
			log.error("MySQL on %s: %s", cursor.statement, err)
		else:
			self.cnx.commit()
		
		cursor.close()

#=========================================================================================================
#					 A U X I L I A R   M E T H O D S
#=========================================================================================================
	def is_an_activity(self, name):
		if (name != 'system') and (name != 'journal') and (name != 'network') and (name != 'shell'):
			return True
		else:	
			return False

	def date_to_ts(self, date):
		return datetime.strptime(date, "%Y-%m-%d")


			
