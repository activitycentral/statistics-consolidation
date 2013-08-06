import logging
from datetime import datetime

import mysql.connector
from mysql.connector import errorcode

import sqlalchemy as sa
from sqlalchemy.sql import func

log = logging.getLogger("stats-consolidation")


class Connection(object):
    """Transitional class"""
    def __init__(self, engine):
        self._connection = engine.connect()

    def cursor(self):
        return self._connection

    def close(self):
        return self._connection.close()

    def commit(self):
        return None


class DB_Stats:
    TABLES={}

    TABLES['Usages'] = (
        "CREATE TABLE `Usages` ("
        "   `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        "   `user_hash` CHAR(40) NOT NULL,"
        "   `resource_name` CHAR(80),"
        "   `start_date` TIMESTAMP NOT NULL,"
        "   `data_type` CHAR (30) NOT NULL,"
        "   `data` INTEGER NOT NULL,"
        "   PRIMARY KEY (`user_hash`,`start_date`,`resource_name`, `data_type`)"
        "   )")

    TABLES['Resources'] = (
        "CREATE TABLE Resources ("
        "   `name` CHAR(250),"
        "   PRIMARY KEY (name)"
        "   )")

    TABLES['Users'] = (
        "CREATE TABLE Users("
        "       `hash` CHAR (40) NOT NULL,"
        "       `uuid` CHAR (32) NOT NULL,"
        "   `machine_sn` CHAR(80),"
        "   `age` INTEGER NOT NULL,"
        "   `school` CHAR(80),"
        "   `sw_version` CHAR (80),"
        "   `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        "   PRIMARY KEY (hash)"
        "   )")

    TABLES['Runs'] = (
        "CREATE TABLE Runs("
        "   `last_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP "
        ")")


    def __init__(self,  db_name, user, password):
        self.db_name  = db_name
        self.user = user
        self.password = password

    def _metadata(self):
        metadata = sa.MetaData()

        # TABLES['Usages'] = (
        # "CREATE TABLE `Usages` ("
        # "   `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        # "   `user_hash` CHAR(40) NOT NULL,"
        # "   `resource_name` CHAR(80),"
        # "   `start_date` TIMESTAMP NOT NULL,"
        # "   `data_type` CHAR (30) NOT NULL,"
        # "   `data` INTEGER NOT NULL,"
        # "   PRIMARY KEY (`user_hash`,`start_date`,`resource_name`, `data_type`)"
        # "   )"
        Usages = sa.Table('Usages', metadata,
            sa.Column('ts', sa.TIMESTAMP,
                server_default=func.current_timestamp(),
                server_onupdate=func.current_timestamp()),
            sa.Column('user_hash', sa.CHAR(40), nullable=False, primary_key=True),
            sa.Column('resource_name', sa.CHAR(80), primary_key=True),
            sa.Column('start_date', sa.TIMESTAMP, nullable=False, primary_key=True),
            sa.Column('data_type', sa.CHAR(30), nullable=False, primary_key=True),
            sa.Column('data', sa.INTEGER, nullable=False),
        )

        # TABLES['Resources'] = (
        # "CREATE TABLE Resources ("
        # "   `name` CHAR(250),"
        # "   PRIMARY KEY (name)"
        # "   )"
        Resources = sa.Table('Resources', metadata,
            sa.Column('name', sa.CHAR(250), primary_key=True),
        )

        # TABLES['Users'] = (
        # "CREATE TABLE Users("
        # "       `hash` CHAR (40) NOT NULL,"
        # "       `uuid` CHAR (32) NOT NULL,"
        # "   `machine_sn` CHAR(80),"
        # "   `age` INTEGER NOT NULL,"
        # "   `school` CHAR(80),"
        # "   `sw_version` CHAR (80),"
        # "   `ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
        # "   PRIMARY KEY (hash)"
        # "   )"
        Users = sa.Table('Users', metadata,
            sa.Column('hash', sa.CHAR(40), nullable=False, primary_key=True),
            sa.Column('uuid', sa.CHAR(32), nullable=False),
            sa.Column('machine_sn', sa.CHAR(80)),
            sa.Column('age', sa.INTEGER, nullable=False),
            sa.Column('school', sa.CHAR(80)),
            sa.Column('sw_version', sa.CHAR(80)),
            sa.Column('ts', sa.TIMESTAMP,
                server_default=func.current_timestamp(),
                server_onupdate=func.current_timestamp()),
        )

        # TABLES['Runs'] = (
        # "CREATE TABLE Runs("
        # "   `last_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP "
        # ")"
        Runs = sa.Table('Runs', metadata,
            sa.Column('ts', sa.TIMESTAMP,
                server_default=func.current_timestamp(),
                server_onupdate=func.current_timestamp()),
        )
        return metadata

    def _create_tables(self, engine):
        metadata = self._metadata()
        metadata.create_all(self._get_engine())

    def create_tables(self, cursor):
        for name, ddl in self.TABLES.iteritems():
            try:
                log.info('Creating table %s:', name)
                cursor.execute(ddl)
            except sa.exc.DBAPIError as err:
                raise err
            else:
                log.info('Table %s created', name)

    def create (self):
        self.connect()
        cursor = self.cnx.cursor()
        try:
            self.create_tables(cursor)
        except sa.exc.DBAPIError as err:
            raise Exception ("Error: {0}".format(err))

    def close (self):
        self.cnx.close()

    def _get_engine(self):
        database_url = 'mysql+mysqlconnector://{user}:{password}@localhost/{db_name}'
        engine = sa.create_engine(database_url.format(
            user=self.user,
            password=self.password,
            db_name=self.db_name)
        )
        return engine

    def connect (self):
        self.cnx = Connection(self._get_engine())
        return self.cnx

#=========================================================================================================
#               Q U E R I E S   S A V E   M E T H O D S
#=========================================================================================================

    def store_activity_uptime (self, rrd):
        self.store_activity_time (rrd, 'uptime')

    def store_activity_focus_time (self, rrd):
        self.store_activity_time(rrd, 'active')



    def store_activity_time(self, rrd, data_type):

        self.store_resource(rrd.get_name())
        self.store_user(rrd)

        cursor = self.cnx.cursor()
        select = ("SELECT * FROM Usages WHERE "
                "user_hash = %s AND "
                "resource_name = %s AND "
                "start_date = %s AND "
                "data_type = %s")
        update = ("UPDATE Usages SET data = %s WHERE "
                "user_hash = %s AND "
                "resource_name = %s AND "
                "start_date = %s AND "
                "data_type = %s")
        insert = ("INSERT INTO Usages "
                "(user_hash, "
                "resource_name, "
                "start_date, "
                "data_type, "
                "data) "
                "VALUES (%s, %s, %s, %s ,%s) ")

        for d in rrd.get_last_value_by_interval(data_type):
            info_sel = (rrd.get_user_hash(), rrd.get_name() , datetime.fromtimestamp(float(d[0])), data_type)
            try:
                """Verify if this activity has an entry already at the same start_date"""
                result_proxy = cursor.execute (select, info_sel)
                result = result_proxy.fetchone()

                if result != None:
                    log.info('Update %s \'%s\' entry for resource \'%s\' ', data_type, d[1], rrd.get_name())
                    info_up = (d[1], rrd.get_user_hash(), rrd.get_name() , datetime.fromtimestamp(float(d[0])), data_type)
                    cursor.execute(update, info_up)
                else:
                    log.info('New %s \'%s\' entry for resource \'%s\'', data_type, d[1], rrd.get_name())
                    info_ins = (rrd.get_user_hash(), rrd.get_name() , datetime.fromtimestamp(float(d[0])), data_type, d[1])
                    cursor.execute(insert, info_ins)

                self.cnx.commit()

            except sa.exc.DBAPIError as err:
                            log.error('MySQL on store_activiy_time()%s: %s %s', data_type, 'cursor.statement', err)
        # cursor.close()  # Not need


    def store_resource(self, resource_name):
        cursor = self.cnx.cursor()
        op = ("SELECT name FROM Resources WHERE name = %s")
        params = (resource_name,)
        try:
            result_proxy = cursor.execute(op, params)
            result = result_proxy.fetchone()
            if result != None:
                log.debug('Resource %s already present in DB', resource_name)
            else:
                insert = ("INSERT INTO Resources (name) VALUES (%s)")
                info = (resource_name, )
                cursor.execute(insert, info)
                self.cnx.commit()
                log.info('New Resource %s stored in DB', resource_name)
        except sa.exc.DBAPIError as err:
            log.error('MySQL on store_resource:  %s %s', 'cursor.statement', err)

        # cursor.close()  # Not need

    def store_user (self, rrd):
        cursor = self.cnx.cursor()
        op = ("SELECT hash FROM Users WHERE hash = %s")
        params = (rrd.get_user_hash(), )
        try:
            result_proxy = cursor.execute(op, params)
            result = result_proxy.fetchone()
            if result != None:
                log.debug('User %s already in DB', rrd.user_hash)
            else:
                insert = ("INSERT INTO Users (hash, uuid, machine_sn, age, school, sw_version) VALUES (%s, %s, %s, %s, %s, %s)")
                params = (rrd.get_user_hash(), rrd.get_uuid(), rrd.get_sn(), rrd.get_age(), rrd.get_school(), "1.0.0")
                cursor.execute(insert, params)
                self.cnx.commit()
                log.debug('New User %s stored in DB', rrd.user_hash)
        except sa.exc.DBAPIError as err:
            log.error('MySQL on store_user %s %s', 'cursor.statement', err)

        # cursor.close()  # Not need



    def update_last_record (self):
        cursor = self.cnx.cursor()
        res = 0
        op = ("SELECT * FROM Runs")
        try:
            result_proxy = cursor.execute(op)
            result = result_proxy.fetchone()

            if result != None:
                op = ("UPDATE Runs SET last_ts = CURRENT_TIMESTAMP")
                cursor.execute(op)
                self.cnx.commit()
            else:
                op = ("INSERT INTO Runs VALUES(CURRENT_TIMESTAMP)")
                cursor.execute(op)
                self.cnx.commit()
            log.info("Save last record");
        except sa.exc.DBAPIError as err:
            log.error('MySQL on update_last_record: %s %s', 'cursor.statement', err)
            res = -1

        # cursor.close()  # Not need
        return res

    def get_date_last_record (self):
        cursor = self.cnx.cursor()
        op = ("SELECT UNIX_TIMESTAMP ((SELECT last_ts FROM Runs))")
        try:
            result_proxy = cursor.execute(op)
            result = result_proxy.fetchone()
            if result != None and result[0] != None:
                log.info('Last record: %s', str(datetime.fromtimestamp (float (result[0]))))
                return result[0]
            else:
                log.info('Last date record is None')
                return 0
        except sa.exc.DBAPIError as err:
            log.error('MySQL on get_date_last_record: %s %s','cursor.statement', err)
        except Exception as e:
            log.error(e)
            raise Exception ("get_date_last_record: {0}".format(e))
        # cursor.close()  # Not need





#=========================================================================================================
#               R E P O R T   M E T H O D S
#=========================================================================================================
    def rep_activity_time (self, start, end, activity, school=None):
        uptime_last=0
        activity_name=''
        focus = 0
        uptime = 0
        ts_start = self.date_to_ts(start)
        ts_end   = self.date_to_ts(end)

        cursor1 = self.cnx.cursor()
        cursor2 = self.cnx.cursor()
        try:
            if school != None:
                select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s)  AND (start_date > %s) AND (start_date < %s) AND (data_type = %s) AND (user_hash = %s)"

                log.debug('Activiy time by school: %s', school)
                """ Get user hash from a School"""
                result1 = cursor1.execute ("SELECT hash FROM Users WHERE school = %s", (school,))
                user_hashes = result1.fetchall()
                for user_hash in user_hashes:
                    log.debug('user Hash: %s', user_hash[0])
                    params_focus = (activity, ts_start, ts_end, 'active', user_hash[0])
                    params_uptime = (activity, ts_start, ts_end, 'uptime', user_hash[0])

                    result2 = cursor2.execute(select_usage, params_focus)
                    focus = float (result2.fetchone()[0]) + focus
                    result2 = cursor2.execute(select_usage, params_uptime)
                    uptime = float (result2.fetchone()[0]) + uptime

            else:
                select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s)  AND (start_date > %s) AND (start_date < %s) AND (data_type = %s)"
                params_focus = (activity, ts_start, ts_end, 'active')
                params_uptime = (activity, ts_start, ts_end, 'uptime')
                result2 = cursor2.execute(select_usage, params_focus)
                focus = float(result2.fetchone()[0])
                result2 = cursor2.execute(select_usage, params_uptime)
                uptime = float(result2.fetchone()[0])

            log.debug('Times of (%s) from: %s -> %s: Uptime: %s, Focus: %s', activity, start, end, uptime, focus)


            # cursor1.close()  # Not need
            # cursor2.close()  # Not need
            return (uptime, focus)
        except sa.exc.DBAPIError as err:
            log.error('MySQL on rep_activity_time %s', err)
        except Exception as e:
            log.error('MySQL on rep_activity_time : %s', e)
        return (None, None)



    def rep_get_activities (self, start, end, school=None, desktop='any'):
        res_list = list();
        cursor1 = self.cnx.cursor()
        cursor2 = self.cnx.cursor()
        cursor3 = self.cnx.cursor()

        if desktop == 'gnome':
            result2 = cursor2.execute("SELECT name FROM Resources WHERE name REGEXP 'application'")
        elif desktop == 'sugar':
            result2 = cursor2.execute("SELECT name FROM Resources WHERE name REGEXP 'activity'")
        else:
            result2 = cursor2.execute("SELECT name FROM Resources")

        resources = result2.fetchall()

        try:
            if school != None:
                log.debug('Most activiy used by school: %s', school)
                """ Get user hash from a School"""
                result1 = cursor1.execute ("SELECT hash FROM Users WHERE school = %s", (school,))
                user_hashes = result1.fetchall()
                """ Cursor for select resources from Uages table"""
                select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s) AND (start_date > %s) AND (start_date < %s) AND (data_type = 'active') AND (user_hash = %s)"
            else:
                log.debug('Most activiy used')
                """ Cursor for select resources from Uages table"""
                select_usage = "SELECT SUM(data) FROM Usages WHERE (resource_name = %s) AND (start_date > %s) AND (start_date < %s) AND (data_type = 'active')"



            ts_start = self.date_to_ts(start)
            ts_end   = self.date_to_ts(end)


            for resource in resources:
                log.debug('Resource: %s', resource[0])
                if self.is_an_activity (resource[0]):
                    if school != None:
                        for user_hash in user_hashes:
                            log.debug('user Hash: %s', user_hash[0])
                            result3 = cursor3.execute(select_usage, (resource[0], ts_start, ts_end, user_hash[0]))
                            focus = result3.fetchone()[0]
                            if focus == None: focus = 0

                            log.debug('Focus time: %s', focus)
                            res_list.append((resource[0], focus))
                    else:
                        result3 = cursor3.execute(select_usage, (resource[0], ts_start, ts_end))
                        focus = result3.fetchone()[0]
                        if focus == None: focus = 0
                        log.debug('Focus time: %s', focus )
                        res_list.append((resource[0], focus))

        except sa.exc.DBAPIError as err:
            log.error('MySQL on most_activity_used %s', err)
        except Exception as e:
            log.error('most_activity_used  Fail: %s', e)
        # cursor1.close()  # Not need
        # cursor2.close()  # Not need
        # cursor3.close()  # Not need
        log.debug ('Activities: %s', sorted(res_list, key=lambda x: x[1], reverse=True))
        return sorted(res_list, key=lambda x: x[1], reverse=True)



    def rep_frequency_usage (self, start, end, school=None):
        cursor1 = self.cnx.cursor()
        cursor2 = self.cnx.cursor()
        user_hashes=()
        time = 0
        try:
            ts_start = self.date_to_ts(start)
            ts_end   = self.date_to_ts(end)

            if school != None:
                log.debug('Frequency usage by school: %s', school)
                """ Get user hash from a School"""
                result1 = cursor1.execute ("SELECT hash FROM Users WHERE school = %s", (school,))
                user_hashes = result1.fetchall()

                for user_hash in user_hashes:
                    result2 = cursor2.execute("SELECT SUM(data) FROM Usages WHERE (resource_name = 'system') AND (start_date > %s) AND (start_date < %s) AND (data_type = 'uptime') AND (user_hash = %s)", (ts_start, ts_end, user_hash[0]))
                    res = result2.fetchone()
                    if res != None and res[0] != None:
                        time = float (res[0]) + time
            else:
                log.debug('Frequency usage')
                result1 = cursor1.execute ("SELECT hash FROM Users")
                user_hashes = result1.fetchall()
                result2 = cursor2.execute("SELECT SUM(data) FROM Usages WHERE (resource_name = 'system') AND (start_date > %s) AND (start_date < %s) AND (data_type = 'uptime')", (ts_start, ts_end))
                time = result2.fetchone()[0]

            return (time, len(user_hashes))


        except sa.exc.DBAPIError as err:
            log.error("MySQL on %s: %s", 'cursor.statement', err)
        # cursor1.close()  # Not need
        # cursor2.close()  # Not need


    def rep_update_school(self, machine_sn, school):
        cursor = self.cnx.cursor()
        try:
            log.debug("Set school name: %s to user with machine_sn: %s", school, machine_sn)
            cursor.execute ("UPDATE Users SET school = %s WHERE machine_sn = %s", (school, machine_sn))
        except sa.exc.DBAPIError as err:
            log.error("MySQL on %s: %s", 'cursor.statement', err)
        else:
            self.cnx.commit()

        # cursor.close()  # Not need

#=========================================================================================================
#                    A U X I L I A R   M E T H O D S
#=========================================================================================================
    def is_an_activity(self, name):
        if (name != 'system') and (name != 'journal') and (name != 'network') and (name != 'shell'):
            return True
        else:
            return False

    def date_to_ts(self, date):
        return datetime.strptime(date, "%Y-%m-%d")



