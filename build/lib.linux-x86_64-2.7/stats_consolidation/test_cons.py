import stats_consolidation
from stats_consolidation import *

db = DB_Stats('statistics', 'root', 'gustavo')
db.create();

con = Consolidation('/home/gustavo/AC/server_stats/sugar-stats/rrd', db)

con.process_rrds()
