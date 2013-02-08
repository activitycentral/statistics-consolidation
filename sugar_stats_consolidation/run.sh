#!/bin/bash
DB_USER="root"
DB_PASS="gustavo"
DB_NAME="statistics"
RRD_PATH=~/AC/school-server/sugar-stats-debian/rrd
LOG_PATH=./

./consolidation_run --db_user $DB_USER --db_pass $DB_PASS --db_name $DB_NAME --rrd_path $RRD_PATH --log_path $LOG_PATH
