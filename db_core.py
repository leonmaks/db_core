import logging
import time
import psycopg2
import psycopg2.errorcodes

import tittles as t


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s: %(asctime)s %(module)s:%(lineno)d(%(funcName)s) %(message)s", level=logging.DEBUG)


_log = logging.getLogger(__name__)


RECONNECT_ATTEMPTS = 0
RECONNECT_PAUSE = 500
PG_DEFAULT_PORT = "5432"


def debug_statement(**kwargs):
    if kwargs.get("debug") == "statement" and not kwargs.get("no_logging"): return True


def apply_statement(**kwargs):
    if kwargs.get("apply") == "statement": return True


class Db():

    def __init__(self, parms, **kwargs):
        self.__conn = None
        self.__user = None
        self.__pass = None
        self.__db = None
        self.__host = None
        self.__port = None
        self.__set_parms(parms, **kwargs)

    def __set_parms(self, parms, **kwargs):
        changed_ = 0

        def __set_if_changed(val, dic, key, default):
            nonlocal changed_
            changed_val_ = dic.get(key, default)
            if changed_val_ != val: changed_ += 1; return changed_val_
            return val

        # USER
        self.__user = __set_if_changed(self.__user, parms, "USER", None)
        if not self.__user: raise t.ConfigError("USER is not defined")
        # PASSWORD
        self.__pass = __set_if_changed(self.__pass, parms, "PASSWORD", None)
        if not self.__pass: raise t.ConfigError("PASSWORD is not defined")
        # DB
        self.__db = __set_if_changed(self.__db, parms, "DATABASE", kwargs.get("database_equals_user") and self.__user or "postgres")
        # HOST
        self.__host = __set_if_changed(self.__host, parms, "HOST", "localhost")
        # PORT
        self.__port = __set_if_changed(self.__port, parms, "PORT", PG_DEFAULT_PORT)

        return changed_ and True or False

    def set_parms(self, parms, **kwargs):
        # Set parameters
        if self.__set_parms(parms, **kwargs):
            # Close connection if any parameter updated
            _log.debug("closing db connection")
            self.close(**kwargs)

    def is_connected(self):
        if self.__conn and self.__conn.closed == 0: return True
        return False

    def __str__(self):
        return "%s @ %s : %s (connected=%s)" % (
            self.__user == self.__db and self.__user or "%s(%s)" % (self.__user, self.__db),
            self.__host, self.__port, self.is_connected(), )

    def close(self, **kwargs):
        if self.__conn:
            if debug_statement(**kwargs): _log.debug("Closing connection")
            self.__conn.close()
            self.__conn = None
        return self

    def __get_conn(self, **kwargs):
        if not self.__conn: self.connect(**kwargs)
        return self.__conn

    def encoding(self):
        return self.__get_conn().encoding

    def connect(self, **kwargs):
        if debug_statement(**kwargs):
            _log.debug("Connecting to %s, reopen=%s." % (self, kwargs.get("reopen")))
        if self.__conn:
            if kwargs.get("reopen"):
                self.close(**kwargs)
            else:
                if debug_statement(**kwargs):
                    _log.debug("Already connected, reopen=%s." % kwargs.get("reopen"))
                return self.__conn # Nothing to do - already connected
        try:
            self.__conn = psycopg2.connect(
                "".join([
                    "host=", self.__host,
                    " dbname=", self.__db,
                    " user=", self.__user,
                    " password=", self.__pass,
                    " port=", self.__port,
                ])
            )
            if debug_statement(**kwargs): _log.debug("Connected to %s" % self)
        except psycopg2.Error as e_:
            raise t.DbConnectError("Cannot connect to %s: %s" % (self, e_))
        return self.__conn

    def commit(self):
        if self.__conn: self.__conn.commit()
        return self

    def rollback(self):
        if self.__conn: self.__conn.rollback()
        return self

    def cursor(self, **kwargs):
        return self.__get_conn(**kwargs).cursor()

    def cursor_close(self, cursor):
        if cursor: cursor.close()

    def __exec_w_reconn(self, stmt, args=None, **kwargs):
        attempts_ = kwargs.get("reconnect_attempts", RECONNECT_ATTEMPTS)
        pause_ = kwargs.get("reconnect_pause", RECONNECT_PAUSE)
        stmt_ = t.lotovts(stmt)
        csr_ = None
        while attempts_ >= 0:
            try:
                if debug_statement(**kwargs):
                    _log.debug("STMT [%s] ARGS %s (attempts=%s, pause=%s)" % (stmt_, args, attempts_, pause_))
                csr_ = self.__get_conn(**kwargs).cursor()
                csr_.execute(stmt_, args)
                attempts_ = -1 # Stop reconnecting on success
            except psycopg2.Error as e_:
                # TODO - test exception handling when closing connection
                # Check ignore error codes
                def error_ignore(e_, **kwargs):
                    ignore_errs_ = kwargs.get("ignore_errs")
                    if ignore_errs_ and (e_.pgcode in ignore_errs_): return True
                if e_.pgcode and error_ignore(e_, **kwargs):
                    _log.warning("Can't execute (ignored) STMT [%s] ARGS %s (connected=%s, pgcode=%s, class=%s): %s" % (
                        stmt_, args, self.is_connected(), e_.pgcode, e_.__class__.__name__, e_))
                    break
                # If error not related to closed connection
                # or number of attemts exhausted - raise exception
                if not self.__get_conn().closed or attempts_ <= 0 or e_.pgcode in ("25P02", ):
                    raise t.DbExecuteError("Can't execute STMT [%s] ARGS %s (connected=%s, pgcode=%s, class=%s): %s" % (
                        stmt_, args, self.is_connected(), e_.pgcode, e_.__class__.__name__, e_))
                # ... else log warning and keep trying execution
                if not kwargs.get("no_logging"):
                    _log.warning("Can't execute STMT [%s] ARGS %s (connected=%s, pgcode=%s, class=%s): %s" % (
                        stmt_, args, self.is_connected(), e_.pgcode, e_.__class__.__name__, e_))
                self.cursor_close(csr_)
                time.sleep(pause_ / 1000.0)
                self.connect(reopen=True, **kwargs)
                attempts_ -= 1
        return csr_

    def select(self, stmt, args=None, **kwargs):
        csr_ = self.__exec_w_reconn(stmt, args, **kwargs)
        return csr_

    def select_one(self, stmt, args=None, **kwargs):
        csr_ = None
        try:
            csr_ = self.select(stmt, args, **kwargs)
            return csr_.fetchone()
        finally: csr_ and csr_.close()

    def select_all(self, stmt, args=None, **kwargs):
        csr_ = None
        try:
            csr_ = self.select(stmt, args, **kwargs)
            return csr_.fetchall()
        finally: csr_ and csr_.close()

    def execute(self, stmt, args=None, **kwargs):
        csr_ = None
        rowcount_ = -1
        try:
            csr_ = self.__exec_w_reconn(stmt, args, **kwargs)
            rowcount_ = csr_.rowcount
            if kwargs.get("commit") == "statement": self.commit()
        finally:
            self.cursor_close(csr_)
        return rowcount_

    def execute_batch(self, batch, **kwargs):
        rowcount_ = -1
        for s_ in batch:
            rowcount_ += self.execute(s_, **kwargs)
        if kwargs.get("commit") == "batch": self.commit()
        return rowcount_


def test():
    import time

    db_parms_ = {
        "USER": "ffba_backup",
        "PASSWORD": "f__",
    }

    conn_ = Db(db_parms_, database_equals_user=True, debug="statement")
    conn_.connect(debug="statement")

    _log.debug("setting parms without changes...")
    conn_.set_parms(db_parms_, database_equals_user=True)

    db_parms_ = {
        "USER": "ffba_170908",
        "PASSWORD": "f__",
    }

    _log.debug("setting changed parms...")
    conn_.set_parms(db_parms_)
    conn_.connect(debug="statement")

    time.sleep(20) # kill DB connection during the pause

    conn_.execute("DROP TABLE NON_EXISTENT_TABLE", ignore_errs=(psycopg2.errorcodes.UNDEFINED_TABLE), reconnect_attempts=1, debug="statement")

    # csr_ = None
    # try:
    #     csr_ = db_.select("select * from t_records where id = %s", (10, ), debug="statement")
    #     for r_ in csr_.fetchall():
    #         _log.debug("record: %s" % (r_,))
    # finally: csr_ and csr_.close()


if __name__ == "__main__":
    test()
