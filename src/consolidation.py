import os
import rrd_files
import db
import argparse

from rrd_files import *
from db import *

class Consolidation:
	
	def __init__(self, path, db):
		self.base_path = path
		self.date_start = db.get_date_last_record()
		self.db = db	
	def process_rrds (self):
		id_hash_list = os.listdir(unicode(self.base_path))
		if id_hash_list:
			for id_hash in id_hash_list:
				user_hash_list = os.listdir( unicode( os.path.join(self.base_path, id_hash) ) )
				if user_hash_list:
					for user_hash in user_hash_list:
						rrd_list = os.listdir( unicode(os.path.join(self.base_path, id_hash, user_hash)) )
						if rrd_list:
							for rrd in rrd_list: 
								rrd_path = unicode (os.path.join(self.base_path, id_hash, user_hash) )
								rrd_obj = RRD (path=rrd_path, name=rrd, date_start=self.date_start, date_end=None)
								self.db.store_activity_uptime(rrd_obj)
						else:
							print "None rrd file found" + os.path.join(self.base_path, id_hash, user_hash)
				else:
					print "None hash user found on: " + os.path.join(self.base_path, id_hash)	
		else:
			print "None hash ids  found on: " + self.base_path

						
