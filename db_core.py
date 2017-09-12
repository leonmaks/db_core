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
DB_PORT = "5432"


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
        # USER
        self.__user = parms.get("USER", None)
        if not self.__user:
            raise t.ConfigError("USER is not defined")
        # PASSWORD
        self.__pass = parms.get("PASSWORD", None)
        if not self.__pass:
            raise t.ConfigError("PASSWORD is not defined")
        # DATABASE
        self.__db = parms.get("DATABASE", None)
        if not self.__db:
            if kwargs.get("database_equals_user"):
                self.__db = self.__user
            else:
                self.__db = "postgres"
        # HOST
        self.__host = parms.get("HOST", None)
        if not self.__host:
            self.__host = "localhost"
        # PORT
        self.__port = parms.get("PORT", DB_PORT)

    def set_parms(self, parms, **kwargs):
        # Close connection if open
        self.close(**kwargs)
        self.__set_parms(parms, **kwargs)

    def __str__(self):
        return "%s @ %s : %s (connected=%s)" % (
            self.__user == self.__db and self.__user or "%s(%s)" % (self.__user, self.__db),
            self.__host, self.__port, self.__get_conn().closed and False or True,
        )

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

    def is_connected(self):
        return self.__conn and True or False

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
            if debug_statement(**kwargs):
                _log.debug("Connected to %s" % self)
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
                # Check ignore error codes
                def __ignore(**kwargs):
                    ignore_errs_ = kwargs.get("ignore_errs")
                    if ignore_errs_ and e_.pgcode in ignore_errs_: return True
                if __ignore(**kwargs):
                    _log.warning("Can't execute (ignored) STMT [%s] ARGS %s (pgcode=%s, class=%s): %s" % (
                        stmt_, args, e_.pgcode, e_.__class__.__name__, e_))
                    break
                # If error not related to closed connection
                # or number of attemts exhausted - raise exception
                if not self.__get_conn().closed or attempts_ <= 0 or e_.pgcode in ("25P02", ):
                    raise t.DbExecuteError("Can't execute STMT [%s] ARGS %s (pgcode=%s, class=%s): %s" % (
                        stmt_, args, e_.pgcode, e_.__class__.__name__, e_))
                # ... else log warning and keep trying execution
                if not kwargs.get("no_logging"):
                    _log.warning("Can't execute STMT [%s] ARGS %s (pgcode=%s, class=%s): %s" % (
                        stmt_, args, e_.pgcode, e_.__class__.__name__, e_))
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


def __test():

    DB = {
        "USER": "ffba_backup",
        "PASSWORD": "f__",
    }

    db_ = Db(DB, database_equals_user=True, debug="statement")
    db_.connect(debug="statement")
    db_.execute("DROP TABLE NON_EXISTENT_TABLE", ignore_errs=(psycopg2.errorcodes.UNDEFINED_TABLE), debug="statement")

    # csr_ = None
    # try:
    #     csr_ = db_.select("select * from t_records where id = %s", (10, ), debug="statement")
    #     for r_ in csr_.fetchall():
    #         _log.debug("record: %s" % (r_,))
    # finally: csr_ and csr_.close()


if __name__ == "__main__":
    __test()
