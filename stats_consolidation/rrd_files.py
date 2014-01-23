import os
import sys
import logging
import rrdtool
from datetime import datetime


log = logging.getLogger("stats-consolidation")


class RRD:
    hdr_item = 0
    ds_item = 1
    data_item = 2
    DS = {'active': 0, 'buddies': 0, 'instances': 0, 'new': 0, 'resumed': 0, 'uptime': 0}

    def __init__(self, path, name, date_start=None, date_end=None):

        self.rrd_name = name

        if date_start is None or (date_start > rrdtool.last(str(os.path.join(path, name)))):
            self.date_start = str(rrdtool.first(str(os.path.join(path, name))))
        else:
            self.date_start = str(date_start)

        if date_end is None:
            self.date_end = str(rrdtool.last(str(os.path.join(path, name))))
        else:
            self.date_end = str(date_end)

        if float(self.date_start) > float(self.date_end):
            raise Exception(
                "Invalid date_start={0} and date_end={1}".format(str(datetime.fromtimestamp(float(self.date_start))),
                                                                 str(datetime.fromtimestamp(float(self.date_end)))))

        self.user_hash = os.path.split(path)[1]

        self.user_path = os.path.join(
            self.get_first_part_path(path, 3),
            "users",
            "user",
            self.user_hash[:2],
            self.user_hash
        )

        self.uuid = self.get_uuid_from_file(self.user_path)
        self.age = 0
        self.sn = self.get_machine_sn_from_file(self.user_path)
        self.school = "unkown"

        log.debug('*******************************************')
        log.debug('     creating a RRD instance               ')
        log.debug('start: %s', str(datetime.fromtimestamp(float(self.date_start))))
        log.debug('end: %s', str(datetime.fromtimestamp(float(self.date_end))))
        log.debug('PATH: %s', path)
        log.debug('RRD NAME: %s', name)
        log.debug('\n')
        try:
            self.rrd = rrdtool.fetch(str(os.path.join(path, name)), 'AVERAGE', '-r 60', '-s ' + self.date_start,
                                     '-e ' + self.date_end)
        except Exception as e:
            raise Exception("rrdtool.fetch: {0}".format(e))
        log.debug('                   DS                       ')
        for item in self.DS.keys():
            idx = self.get_ds_index(item)
            if idx != -1:
                self.DS[item] = idx
            else:
                log.warning('DS %s not found in header of %s rrd file', item, name)
        log.debug('***********************************************')

    def get_ds_index(self, ds):
        i = 0
        for i in range(len(self.rrd[self.ds_item])):
            if self.rrd[self.ds_item][i] == ds:
                return i
            i = +1
        return -1

    def get_last_value_by_interval(self, ds_name):
        """
        Find several valid record consecutives, the last one is time of the interval.
        Return: a list (start_time, total_time)
        """
        res = list()
        prev_value = 0.0
        i = 0
        found = False

        log.debug('-------Calcule %s -------', ds_name)
        while i < len(self.rrd[self.data_item]):
            value = str(self.rrd[self.data_item][i][self.DS[ds_name]])

            if (value != "None") and (float(value) > 0) and (float(value) >= float(prev_value)):
                prev_value = value
                end = long(self.date_start) + ((i + 1) * 60)
                if found is False:
                    found = True
                    start = long(self.date_start) + ((i + 1) * 60)
            else:
                if found:
                    if self.verify_interrupt(i, ds_name, prev_value):
                        log.debug('%s -> %s : %s', str(datetime.fromtimestamp(float(start))),
                                  str(datetime.fromtimestamp(float(end))),
                                  prev_value)
                        res.append((start, prev_value))
                        found = False
                        prev_value = 0.0
            i = i + 1
        return res
        log.debug('---------------------------------------------------')

    def get_active_by_interval(self):
        return self.get_last_value_by_interval("active")

    def get_uptime_by_interval(self):
        return self.get_last_value_by_interval("uptime")

    def get_name(self):
        return self.rrd_name.partition(".rrd")[0]

    def show_valid_ds(self, ds_name):
        log.debug('------------------- DS %s ---------------------', ds_name)
        i = 0
        while i < len(self.rrd[self.data_item]):
            timestamp = str(long(self.date_start) + ((i + 1) * 60))
            value = str(self.rrd[self.data_item][i][self.DS[ds_name]])

            if value != "None":
                log.debug('%s (%s): %s', str(datetime.fromtimestamp(float(timestamp))),
                          timestamp,
                          value)
            i = i + 1
        log.debug('---------------------------------------------------')

    def get_date_last_record(self):
        return self.date_end

    def set_user_hash(self, u_hash):
        self.user_hash = u_hash

    def get_first_part_path(self, path, idx):
        l = list()
        l.append(path)
        for i in range(idx):
            l.append(os.path.split(l[i])[0])
        return l[idx]

    def get_uuid_from_file(self, path):
        return open(os.path.join(path, "machine_uuid")).next().strip("\"")

    def get_machine_sn_from_file(self, path):
        return open(os.path.join(path, "machine_sn")).next().strip("\"")

    def get_user_hash(self):
        return self.user_hash

    def get_uuid(self):
        return self.uuid

    def get_sn(self):
        return self.sn

    def get_school(self):
        return self.school

    def get_age(self):
        return self.age

    def verify_interrupt(self, idx, ds_name, prev_value):
        """
        For some reason, sometimes for a while activity is running, statistics library register several values as None.
        To detect this behavoir, this function look-up over next records time, and verify if the value is grater than
        last valid value + (interval_numb * 60). If the value es greater, means the activity still running else
            the activity was stopped and starting again.
        """
        i = idx
        j = 0
        while i < len(self.rrd[self.data_item]):
            value = str(self.rrd[self.data_item][i][self.DS[ds_name]])
            if value != "None":
                """
                log.debug( "["+str(j)+ "] current value: " + value + " prev value: " +  str (float (prev_value) + (60 * j)) + " ("+ prev_value+")"
                """
                if float(value) > (float(prev_value) + (60 * j)):
                    return False
                else:
                    return True
            i = i + 1
            j = j + 1

        return True
