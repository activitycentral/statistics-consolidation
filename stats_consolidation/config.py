import ConfigParser
import sys


class Config():
    def __init__(self, config_file):
        config = ConfigParser.ConfigParser()
        fp = config.read(config_file)
        if fp == []:
            msg = "ERROR: %s config file couldn't be parsed\n" % config_file
            sys.stderr.write(msg)
            sys.exit(1)

        self.log_path = config.get('main', 'log_path')
        self.log_level = config.get('main', 'log_level')
        self.db_name = config.get('main', 'db_name')
        self.db_user = config.get('main', 'db_user')
        self.db_pass = config.get('main', 'db_pass')
        self.rrd_path = config.get('main', 'rrd_path')

        try:
            self.dialect = config.get('main', 'db_dialect')
        except ConfigParser.NoOptionError:
            self.dialect = None
