import rrdtool

ret = rrdtool.xport ('--start 1355312020', '--end 1355314500', 
'DEF:x=./xo_stats/abacus.rrd:active:AVERAGE',
'XPORT:x:"Ativitye active"')

if ret:
	print rrdtool.error()
else:
	print "OK"
