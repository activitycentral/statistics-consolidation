import os
import argparse
import logging

from stats_consolidation.db import *
from stats_consolidation.rrd_files import *


log = logging.getLogger("stats-consolidation")


class Consolidation:

    def __init__(self, path, db):
        self.base_path = path
        try:
            self.date_start = db.get_date_last_record()
            if self.date_start == 0:
                self.date_start = None
            self.db = db
        except Exception as e:
            log.error('Exception: %s ', e)

    def process_rrds(self):
        id_hash_list = os.listdir(unicode(self.base_path))
        try:
            if id_hash_list:
                for id_hash in id_hash_list:
                    user_hash_list = os.listdir(unicode(os.path.join(self.base_path, id_hash)))
                    if user_hash_list:
                        for user_hash in user_hash_list:
                            rrd_list = os.listdir(unicode(os.path.join(self.base_path, id_hash, user_hash)))
                            if rrd_list:
                                for rrd in rrd_list:
                                    rrd_path = unicode(os.path.join(self.base_path, id_hash, user_hash))
                                    try:
                                        rrd_obj = RRD(path=rrd_path, name=rrd, date_start=self.date_start, date_end=None)
                                        self.db.store_activity_uptime(rrd_obj)
                                        self.db.store_activity_focus_time(rrd_obj)
                                    except Exception as e:
                                        log.warning('Exception on RRD object instance (%s): \'%s\'', rrd, e)
                            else:
                                log.warning('RRD file not found: %s', os.path.join(self.base_path, id_hash, user_hash))
                    else:
                        log.warning('Hash user direcotory not found: %s', os.path.join(self.base_path, id_hash))
                self.db.update_last_record()
                log.info("End RRDs processing")
            else:
                log.error('Hash ids  not found on: %s', self.base_path)
        except Exception as e:
            log.error('Excpetion processing rrds: \'%s\'', e)
