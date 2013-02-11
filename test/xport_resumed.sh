rrdtool xport --start 1355312020 --end 1355314500 \
DEF:x=./xo_stats/abacus.rrd:resumed:AVERAGE \
XPORT:x:"Ativitye resumed"
