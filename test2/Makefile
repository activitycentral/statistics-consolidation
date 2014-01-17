.PHONY: dropdb crc run test .coverage coverage

MYSQL_ARGS=--user=sc --password=sc
MYSQL_CLI_0=mysql $(MYSQL_ARGS)
MYSQL_CLI=mysql $(MYSQL_ARGS) sc
MYSQL_DUMP=mysqldump $(MYSQL_ARGS) sc

CMD=../stats_consolidation/stats_consolidation_run

PIP=$(VIRTUAL_ENV)/bin/pip
COVERAGE=python $(VIRTUAL_ENV)/bin/coverage run --source=../stats_consolidation,../sql

ALL: requirements coverage

requirements:
	@[ $(VIRTUAL_ENV) ] || { echo "Please, run it inside a virtualenv"; exit 1; }
	@[ -n "`$(PIP) show coverage`" ] || { echo "coverage not installed"; exit 1; }


dropdb:
	-$(MYSQL_CLI) --execute="DROP DATABASE sc;"

createdb:
	-$(MYSQL_CLI_0) --execute="CREATE DATABASE IF NOT EXISTS sc;"

crc:
	$(MYSQL_CLI) --batch < crc.sql > crc_test.txt
	diff crc_test.txt crc.txt

run:
	$(CMD) --config_file test.conf

test: dropdb createdb run crc

clean:
	rm -rf log/*
	rm -f crc_test.txt
	rm -f .coverage
	rm -rf htmlcov/


DBOPTIONS=--db_name sc --db_user sc --db_pass sc --db_dialect postgres

.coverage: dropdb createdb
	$(COVERAGE) $(CMD) --config_file test.conf
	$(COVERAGE) $(CMD) --config_file test.conf
	$(COVERAGE) -a\
		../sql/make_report --query all $(DBOPTIONS) --start_date 2013-01-01 --end_date 2013-12-12
	$(COVERAGE) -a\
		../sql/make_report --query update_school $(DBOPTIONS) --school "TEST" --machine_sn "anonymous"
	$(COVERAGE) -a\
		../sql/make_report --query desktops $(DBOPTIONS) --start_date 2013-01-01 --end_date 2013-12-12
	$(COVERAGE) -a\
		../sql/make_report --query desktops $(DBOPTIONS) --start_date 2013-01-01 --end_date 2013-12-12 --school TEST
	$(COVERAGE) -a\
		../sql/make_report --query desktops $(DBOPTIONS) --start_date 2013-01-01 --end_date 2013-12-12 --desktop gnome
	$(COVERAGE) -a\
		../sql/make_report --query activity_time $(DBOPTIONS) --start_date 2013-01-01 --end_date 2013-12-12 --activity journal --school TEST

coverage: .coverage
	$(VIRTUAL_ENV)/bin/coverage html
