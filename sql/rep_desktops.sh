#!/bin/bash
DB_NAME=statistics
DB_USER=root
DB_PASS=gustavo

QUERY=desktops
DESKTOP=gnome
START_DATE=2013-02-10
END_DATE=2013-02-14


s
LOG_LEVEL=info


./make_report --query $QUERY --desktop $DESKTOP --start_date $START_DATE --end_date $END_DATE --db_name $DB_NAME --db_user $DB_USER --db_pass $DB_PASS --log_level $LOG_LEVEL

