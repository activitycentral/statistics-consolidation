Install Instructions
--------------------

1. Install Python package.

    # python setup.py install

This will install Python module in Python path and `stats_consolidation_run`
script.

This package requires python-rrdtool (1.4.7) and SQLAlchemy (0.8.2).

Additionally, it requires a database driver which depends on RDBMS installed
in the system. For MySQL uses mysql-connector-python (1.0.10) and for
PostgreSQL psycopg2 (2.5.1).

    |------------|------------------------|----------------------|
    | RDBMS      | Library                | Dialect+driver       |
    |------------|------------------------|----------------------|
    | MySQL      | mysql-connector-python | mysql+mysqlconnector |
    | POstgreSQL | psycopg2               | postgres             |
    |------------|------------------------|----------------------|


2. Create database.

`stats_consolidation_run` recreates tables but the database must exist
previously.


3. Edit configuration file with appropriate values and install in default path.


This an example configuration file:

    [main]
    db_user=user
    db_pass=password
    db_name=database
    db_dialect=postgres

    rrd_path=/var/lib/sugar-stats/rrd
    log_path=/var/log
    log_level=debug

Then copy to default configuration path:

    # cp stats-consolidation.conf /etc/stats-consolidation.conf


4. Activate crontab.

Check the actual path of main script.

    $ which stats_consolidation_run
    /usr/local/bin/stats_consolidation_run

Configure the following entry on crontab with correct arguments:

    0 3 * * * root /usr/local/bin/stats_consolidation_run --db_user=user --db_pass=password db_name=database db_dialect=postgres rrd_path=/var/lib/sugar-stats/rrd log_path=/var/log

