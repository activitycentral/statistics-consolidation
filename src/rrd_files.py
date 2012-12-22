import rrdtool


class RRD:

	hdr_item  = 0
	ds_item   = 1
	data_item = 2
	DS = {'active':0, 'buddies':0, 'instances':0, 'new':0, 'resumed':0, 'uptime':0}

	def __init__(self, rrd_path, rrd_name, date_start, date_end):

		self.rrd_name = rrd_name

		if date_start == None:
			self.date_start = str(rrdtool.first(rrd_path +"/"+ rrd_name))
		else:
			self.date_start = str(date_start)
			

		if date_end == 0:
			self.date_end = str(rrdtool.last(rrd_path +"/"+ rrd_name))
		else:
			self.date_end   = str(date_end)

		print "start: " + self.date_start
		print "end: " + self.date_end
						
		self.rrd = rrdtool.fetch (rrd_path +"/"+ rrd_name, 'AVERAGE', '-r 60', '-s '+ self.date_start, '-e '+self.date_end)

		for item in self.DS.keys():
			idx = self.get_ds_index (item)
			if idx != -1:
				self.DS[item] = idx
				print item + ": " + str(self.DS[item])
			else:
				print item + " not found in header"


	def get_ds_index(self, ds): 
		i=0
		for i in range (len (self.rrd[self.ds_item])):
			if self.rrd[self.ds_item][i] == ds:
				return i
			i=+1
		return -1

	def get_uptime_by_interval (self):
		ds_name = "uptime"
		res=list()
		print "------------------- Calcule "+ ds_name +"---------------------"
		i=0
		found = False
		while i < len(self.rrd[self.data_item]):
			value     = str(self.rrd[self.data_item][i][self.DS[ds_name]])
			if value != "None":
				uptime = value
				end    = str (long(self.date_start) + ((i+1) * 60))
				if found == False:
					found = True
					start = str (long (self.date_start) + ((i+1) * 60))
			else:
				if found:
					print start + "->" + end + ": " + uptime
					res.append((start, uptime))
					found = False
			i=i+1
		return res
		print "---------------------------------------------------"


	def get_name(self):
		return self.rrd_name.partition(".rrd")[0]

	def show_valid_ds(self, ds_name):
		print "------------------- DS "+ ds_name +"---------------------"
		i=0
		while i < len(self.rrd[self.data_item]):
			timestamp = str (long (self.date_start) + ((i+1) * 60))
			value     = str (self.rrd[self.data_item][i][self.DS[ds_name]])
	
			if value != "None":
				print timestamp+ ": " + value
			i=i+1
		print "---------------------------------------------------"


	def get_last_record(self):
		return self.date_end
