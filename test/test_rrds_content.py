#!/usr/bin/env python

import argparse
from rrd_files import *

parser = argparse.ArgumentParser()
parser.add_argument('--rrd_path',required=False)
parser.add_argument('--rrd_name',required=True)

args = parser.parse_args()

print "==============================       TEST RRD analyze content  ========================================"

if args.rrd_path == None:
	def_path = "/home/olpc/.sugar/default/stats"
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
