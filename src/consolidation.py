import os
import rrd_files
import db

from rrd_files import *
from db import *

class Consolidation:
	
	def __init__(self, path, db):
		self.base_path = path
		self.date_start = db.get_date_last_record()
	
	def process_rrds (self):
		for id_hash in os.listdir(unicode(self.base_path)) :
			for user_hash in os.listdir( unicode( os.path.join(self.base_path, id_hash) ) ):
				print (unicode(os.path.join(self.base_path, id_hash, user_hash)))
				for rrd in os.listdir( unicode(os.path.join(self.base_path, id_hash, user_hash)) ): 
					rrd_path = unicode (os.path.join(self.base_path, id_hash, user_hash) )
					print ("PATH: "+ rrd_path + "file: "+ rrd)
					rrd = RRD (path=rrd_path, name=rrd, date_start=self.date_start, date_end=None)
			
