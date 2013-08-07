.PHONY: dropdb crc run test .coverage coverage

MYSQL_ARGS=--user=sc --password=sc
MYSQL_CLI_0=mysql $(MYSQL_ARGS)
MYSQL_CLI=mysql $(MYSQL_ARGS) sc
MYSQL_DUMP=mysqldump $(MYSQL_ARGS) sc

ALL: coverage

dropdb:
	-$(MYSQL_CLI) --execute="DROP DATABASE sc;"

createdb:
	-$(MYSQL_CLI_0) --execute="CREATE DATABASE IF NOT EXISTS sc;"

crc:
	$(MYSQL_CLI) --batch < crc.sql > crc_test.txt
	diff crc_test.txt crc.txt

run:
	../stats_consolidation/stats_consolidation_run --config_file test.conf

test: dropdb createdb run crc

clean:
	rm -rf log/*
	rm -f crc_test.txt
	rm -f .coverage
	rm -rf htmlcov/

COVERAGE=python ../.venv/bin/coverage run --source=../stats_consolidation,../sql

.coverage: dropdb createdb
	$(COVERAGE)\
		../stats_consolidation/stats_consolidation_run --config_file test.conf
	$(COVERAGE)\
		../stats_consolidation/stats_consolidation_run --config_file test.conf
	$(COVERAGE) -a\
		../sql/make_report --query all --db_name sc --db_user sc --db_pass sc --start_date 2013-01-01 --end_date 2013-12-12
	$(COVERAGE) -a\
		../sql/make_report --query update_school --db_name sc --db_user sc --db_pass sc --school "TEST" --machine_sn "anonymous"
	$(COVERAGE) -a\
		../sql/make_report --query desktops --db_name sc --db_user sc --db_pass sc --start_date 2013-01-01 --end_date 2013-12-12
	$(COVERAGE) -a\
		../sql/make_report --query desktops --db_name sc --db_user sc --db_pass sc --start_date 2013-01-01 --end_date 2013-12-12 --school TEST
	$(COVERAGE) -a\
		../sql/make_report --query desktops --db_name sc --db_user sc --db_pass sc --start_date 2013-01-01 --end_date 2013-12-12 --desktop gnome
	$(COVERAGE) -a\
		../sql/make_report --query activity_time --db_name sc --db_user sc --db_pass sc --start_date 2013-01-01 --end_date 2013-12-12 --activity journal --school TEST

coverage: .coverage
	coverage html
