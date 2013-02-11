
import rrdtool
import os
from datetime import datetime
from time import sleep

activityName = "activity-1"
start_yr= 2011
start_mo= 1
start_dy=1
start_hr=0
start_mi=0
start_se=0

rrdFile="/home/gustavo/AC/"+activityName+".rrd"
date_start_unix =long (datetime(year=start_yr, month=start_mo, day=start_dy, hour=start_hr, minute=start_mi, second=start_se).strftime("%s"))



"""RRD db population"""
i=2
while i<20:
	timestamp = date_start_unix + (i * 30)
	if (i<5):
		val=1	
	elif (i<10):
		val=0
	elif (i<20):
		val=1
	print str(timestamp)+":"+str(val)
	rrdtool.update(rrdFile, '%d:%d' % (timestamp, val))
	i+=1
