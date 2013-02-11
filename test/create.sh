rrdtool create test.rrd                \
            --start 920804400          \
	    --step 1		       \
            DS:speed:COUNTER:2:U:U   \
            RRA:AVERAGE:0.5:1:10
