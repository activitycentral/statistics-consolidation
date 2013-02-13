#!/usr/bin/env python

import argparse
from sugar_stats_consolidation.rrd_files import *

parser = argparse.ArgumentParser()
parser.add_argument('--rrd_path',required=False)
parser.add_argument('--rrd_name',required=True)

args = parser.parse_args()

print "==============================       TEST RRD analyze content  ========================================"

if args.rrd_path == None:
	def_path = "/var/lib/sugar-stats/rrd/ed/ed4f8bd4c24d4f10b7bd6c59add7032b0fbf5dbd"
else: 
	def_path = args.rrd_path

rrd = RRD (path=def_path, name=args.rrd_name)

"""
act_rrd.show_valid_ds("resumed")
act_rrd.show_valid_ds("new")
act_rrd.show_valid_ds("instances")
act_rrd.show_valid_ds("buddies")
"""
rrd.show_valid_ds("uptime")
rrd.show_valid_ds("active")

rrd.get_uptime_by_interval()
rrd.get_active_by_interval()
