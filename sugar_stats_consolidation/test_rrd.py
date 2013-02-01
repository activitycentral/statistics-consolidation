#!/usr/bin/env python

import argparse
from db import *
from rrd_files import *
from db import *

parser = argparse.ArgumentParser()
parser.add_argument('--rrd_path',required=False)
parser.add_argument('--rrd_name',required=True)

args = parser.parse_args()

print "==============================       TEST RRD -> Relational DB  ========================================"
db = DB_Stats('statistics', 'root', 'gustavo')
db.create()

DATE_START =datetime(year=2012, 
			month=12, 
                        day=13, 
                        hour=0, 
                        minute=0, 
                        second=0).strftime("%s")


DATE_END = datetime(year=2012, 
			        month=12, 
                                day=14, 
                                hour=0, 
                                minute=0, 
                                second=0).strftime("%s")

DATE_START = db.get_date_last_record()
DATE_END = datetime.now().strftime("%s")

act_rrd = RRD (path = "/home/gustavo/AC/consolidation/rrds", name=args.rrd_name)
"""
act_rrd = RRD (path = "/home/gustavo/AC/consolidation/rrds", name="abacus.rrd", date_start=DATE_START, date_end=DATE_END)
"""
"""
act_rrd.show_valid_ds("resumed")
act_rrd.show_valid_ds("new")
act_rrd.show_valid_ds("instances")
act_rrd.show_valid_ds("buddies")
data = {}
db.store_activity_uptime(act_rrd)
"""
act_rrd.show_valid_ds("uptime")
act_rrd.show_valid_ds("active")

act_rrd.get_uptime_by_interval()
act_rrd.get_active_by_interval()
