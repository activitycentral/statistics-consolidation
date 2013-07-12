.PHONY: dropdb crc run test

MYSQL_ARGS=--user=sc --password=sc
MYSQL_CLI=mysql $(MYSQL_ARGS) sc 
MYSQL_DUMP=mysqldump $(MYSQL_ARGS) sc

dropdb:
	$(MYSQL_CLI) --execute="DROP DATABASE sc;"

crc:
	$(MYSQL_CLI) --batch < crc.sql > crc_test.txt
	diff crc_test.txt crc.txt

run:
	../stats_consolidation/stats_consolidation_run --config_file test.conf

test: dropdb run crc

clean:
	rm -rf log/*
	rm crc_test.txt
