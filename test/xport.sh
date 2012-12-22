  rrdtool xport \
          --start 1293840000 --end 1293847770 \
          DEF:a=activity-1.rrd:uptime:AVERAGE \
	  XPORT:a:"Uptime" \
