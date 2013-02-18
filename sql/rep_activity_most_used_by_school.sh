#!/bin/bash
DB_NAME=statistics
DB_USER=root
DB_PASS=gustavo

QUERY=activity_most_used
SCHOOL=Ceibal
START_DATE=2013-02-10
END_DATE=2013-02-14



./make_report --query $QUERY --school $SCHOOL --start_date $START_DATE --end_date $END_DATE --db_name $DB_NAME --db_user $DB_USER --db_pass $DB_PASS

