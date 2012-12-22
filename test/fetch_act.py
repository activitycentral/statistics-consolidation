import rrdtool
from datetime import datetime

hdr_item  = 0
ds_item   = 1
data_item = 2


DS = {'active':0, 'buddies':0, 'instances':0, 'new':0, 'resumed':0, 'uptime':0}

RRD_FILE = "abacus.rrd"
START_DATE = date_start_unix =long (datetime(year=2012, 
			        month=12, 
                                day=13, 
                                hour=0, 
                                minute=0, 
                                second=0).strftime("%s"))

END_DATE = date_start_unix =long (datetime(year=2012, 
			        month=12, 
                                day=14, 
                                hour=0, 
                                minute=0, 
                                second=0).strftime("%s"))

"""///////////////////////////////// FUNCTIONS ////////////////////////////////////"""

def get_ds_index(ds): 
	i=0
	for i in range (len (l[ds_item])):
		if l[ds_item][i] == ds:
			return i
		i=+1
	return -1

def calc_uptime ():
	ds_name = "uptime"
	print "------------------- Calcule "+ ds_name +"---------------------"
	i=0
	found = False
	while i < len(l[data_item]):
		value     = str(l[data_item][i][DS[ds_name]])
		if value != "None":
			uptime = value
			end    = str (start_time + ((i+1) * 60))
			if found == False:
				found = True
				start = str (start_time + ((i+1) * 60))
		else:
			if found:
				print start + "->" + end + ": " + uptime
				found = False
		i=i+1
print "---------------------------------------------------"

def show_valid_ds(ds_name):
	print "------------------- DS "+ ds_name +"---------------------"
	i=0
	while i < len(l[data_item]):
		timestamp = str (start_time + ((i+1) * 60))
		value     = str(l[data_item][i][DS[ds_name]])
	
	
		print timestamp+ ": " + value
		"""
		if value != "None":
			print timestamp+ ": " + value
			print "Intervalo:"
			stime = timestamp
			while (i < len(l[data_item])) & (value != "None"):
				timestamp = str (start_time + (i * 60))
				value     = str(l[data_item][i][get_ds_index(DS['active'])])
				print timestamp+ ": " + value
				i=i+1 
			"""
		i=i+1
print "---------------------------------------------------"

"""///////////////////////////////// FUNCTIONS ////////////////////////////////////"""




l=rrdtool.fetch ('./xo_stats/'+RRD_FILE, 'AVERAGE', '-r 60', '-s '+str(START_DATE), '-e '+str(END_DATE))
print "------------------------------ RRD FILE -------------------------------"
print "-----------------------------------------------------------------------"

start_time = l[hdr_item][0]
end_time   = l[hdr_item][1]
resolution = l[hdr_item][2]


print "------------- Data Sources-----------------"
for item in DS.keys():
	idx = get_ds_index (item)
	if idx != -1:
		DS[item] = idx
		print item + ": " + str(DS[item])
	else:
		print item + " not found in header"
print "--------------------------------------------"
"""
j=0
for item in l:
	i=0
	print "\nITEM " +str(j) + ":"
	while i < len(item):
		print item[i]
		i=i+1
	j= j+1
"""

show_valid_ds("active")
show_valid_ds("uptime")
calc_uptime()
show_valid_ds("resumed")
show_valid_ds("new")
show_valid_ds("instances")
show_valid_ds("buddies")
