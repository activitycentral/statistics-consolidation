from __future__ import print_function
import os
import argparse

import rrd_files
import db
from rrd_files import *
from db import *

class Consolidation:
	
	def __init__(self, path, db):
		self.base_path = path
		self.date_start = db.get_date_last_record()
		self.db = db	
	def process_rrds (self):
		id_hash_list = os.listdir(unicode(self.base_path))
		try:
			if id_hash_list:
				for id_hash in id_hash_list:
					user_hash_list = os.listdir( unicode( os.path.join(self.base_path, id_hash) ) )
					if user_hash_list:
						for user_hash in user_hash_list:
							rrd_list = os.listdir( unicode(os.path.join(self.base_path, id_hash, user_hash)) )
							if rrd_list:
								for rrd in rrd_list: 
									rrd_path = unicode (os.path.join(self.base_path, id_hash, user_hash) )
									try:
										rrd_obj = RRD (path=rrd_path, name=rrd, date_start=self.date_start, date_end=None)
										self.db.store_activity_uptime(rrd_obj)
									except Exception as e:
										print ("warning, coninute ..")
							else:
								print ("RRD file not found: {0}".format(os.path.join(self.base_path, id_hash, user_hash)))
					else:
						print ("None hash user found on: {0}".format(os.path.join(self.base_path, id_hash)))	
			else:
				print ("None hash ids  found on: {0}" + format(self.base_path))
		except Exception as e:
			print ("Error processing rrds: {0}".format(str(e)))
						
