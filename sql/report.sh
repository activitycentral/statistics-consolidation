#!/bin/bash
#QUERY=activity_most_used
QUERY=frequency_usage
START_DATE=2013-02-10
END_DATE=2013-02-14
DB_NAME=statistics
DB_USER=root
DB_PASS=gustavo

./make_report --query $QUERY --start_date $START_DATE --end_date $END_DATE --db_name $DB_NAME --db_user $DB_USER --db_pass $DB_PASS

