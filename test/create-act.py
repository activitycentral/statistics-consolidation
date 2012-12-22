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
date_start = datetime(year=start_yr, month=start_mo, day=start_dy, hour=start_hr, minute=start_mi, second=start_se)

date_start_unix = date_start.strftime("%s")
date_start_cal =  date_start.strftime("%Y:%m:%d %H:%M:%S")

print "RRD"
print "  Path: "+ rrdFile
print "  Date START: unix["+str(date_start_unix)+"], cal["+date_start_cal+"]" 


if os.path.exists(rrdFile):
	print "File: [" + rrdFile + "] are created already" 
else:
	rrdtool.create(rrdFile, "-b "+str(date_start_unix), '-s 1', 'DS:uptime:GAUGE:120:0:1', 'RRA:AVERAGE:0.5:1:1200')

