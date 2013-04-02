#!/bin/bash
DB_NAME=statistics
DB_USER=root
DB_PASS=gustavo

QUERY=activity_most_used
CANT_MAX=10
DESKTOP=any
START_DATE=2013-02-10
END_DATE=2013-03-30



LOG_LEVEL=info


./make_report --query $QUERY --desktop $DESKTOP --cant_max $CANT_MAX --start_date $START_DATE --end_date $END_DATE --db_name $DB_NAME --db_user $DB_USER --db_pass $DB_PASS --log_level $LOG_LEVEL

