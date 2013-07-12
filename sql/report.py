#!/usr/bin/env  python

# Copyright (C) 2012, Gustavo Duarte
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import print_function

from stats_consolidation.db import *



class Report:
	STAT={}
	STAT ['Get_resource_name'] = ("SELECT name FROM `Resources`")

	STAT ['Get_suma_uptime'] = ( "SELECT SUM (`data`) FROM Usages (WHERE `resource_name` = %s AND data_type = 'active')")

	STAT ['Get_frequency_usage'] = ("SELECT SUM(`data`) FROM Usages ((WHERE `resource_name` = `system`) AND (start_date > start) AND (start_date < end))")
	

	def __init__ (self,  db_name, user, password):
		self.db_name  = db_name
		self.user = user
		self.password = password
		"""
		try:
			self.connect()
		except Exception as e:
			print ("INIT:")
		"""
	def connect (self):
		print ("Try connect to db")
		try:
			self.cnx = mysql.connector.connect(user=self.user, password=self.password)
			cursor = self.cnx.cursor()
		    	self.cnx.database = self.db_name
			cursor.close()
		except mysql.connector.Error as err:
			print("CONNECT FAIL {}".format (err))

	def most_activity_use (self):
		print ("most_activity_used")
                try:
			res_tmp =('', 0)
			res = ('', 0)
			cursor = self.cnx.cursor()
                        cursor.execute(self.STAT['Get_resource_name'])
			for (name) in cursor:
				cursor.execute (self.STAT['Get_suma_uptime'], (name,))
				res_tmp =(name, cursor.fetchone())
				print ("activity: {}  uptime: {}".format(res_tmp[0], res_tmpe[1]))
				if res_tmp[1] > res[1]:
					res = res_tmp
                except mysql.connector.Error as err:
			print("Fail {}: {}".format(cursor.statement, err))
		except Exception as e:
                        print("most_activity_used Fail ")
                cursor.close()
		return res

	def frequency_usage(self):
		cursor = self.cnx.cursor()
                try:
			cursor.execute(self.STAT['Get_frequency_usage'])
			res = cursor.fetchone()
		except mysql.connector.Error as err:
                        print("ferquency_usage")
		cursor.close()	

		return res



