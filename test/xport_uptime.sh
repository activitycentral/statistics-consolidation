rrdtool xport --start 1355312020 --end 1355314500 \
DEF:x=/home/gustavo/AC/consolidation/rrds/abacus.rrd:uptime:AVERAGE \
XPORT:x:"Ativity uptime"
