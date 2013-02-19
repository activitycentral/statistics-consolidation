DB_USER="root"
DB_PASS="gustavo"
DB_NAME="statistics"
RRD_PATH=/var/lib/sugar-stats/rrd
APP_PATH=/home/ceibal/ac/statistics-consolidation
LOG_PATH=$APP_PATH/log

echo `date`: Starting consolidation      
$APP_PATH/sugar_stats_consolidation/consolidation_run --db_user $DB_USER --db_pass $DB_PASS --db_name $DB_NAME --rrd_path $RRD_PATH --log_path $LOG_PATH
echo `date`: Ending consolidation        

exit 0
