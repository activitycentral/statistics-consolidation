#!/bin/bash
DB_NAME=statistics
DB_USER=root
DB_PASS=gustavo

QUERY=update_school

LOG_LEVEL=debug
SCHOOL=activitycentral
MACHINE_SN=777788889999

./make_report --query $QUERY --school $SCHOOL --machine_sn $MACHINE_SN --db_name $DB_NAME --db_user $DB_USER --db_pass $DB_PASS --log_level $LOG_LEVEL

