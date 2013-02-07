#!/usr/bin/env python

import sugar_stats_consolidation
from sugar_stats_consolidation.db import *
from sugar_stats_consolidation.rrd_files import *
from sugar_stats_consolidation.consolidation import *

db = DB_Stats('statistics', 'root', 'gustavo')
db.create();

con = Consolidation('/var/lib/sugar-stats/rrd', db)

con.process_rrds()
