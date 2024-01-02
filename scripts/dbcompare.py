import time
import os
from multiprocessing import Process

import mysql.connector

from mysql_compare import MysqlTableCompare

ARGS_SOURCE_DSN = os.environ.get("ARGS_SOURCE_DSN")
ARGS_TARGET_DSN = os.environ.get("ARGS_TARGET_DSN")
ARGS_DATABASES = os.environ.get("ARGS_DATABASES")

_userpass, _hostport = ARGS_SOURCE_DSN.split("@")
_user, _pass = _userpass.split("/")
_host, _port = _hostport.split(":")
args_source_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

_userpass, _hostport = ARGS_TARGET_DSN.split("@")
_user, _pass = _userpass.split("/")
_host, _port = _hostport.split(":")
args_target_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

_databases = ARGS_DATABASES.split(",")
_dbs = ",".join(map(lambda d: f"'{d}'", _databases))

tables: list[tuple[str, str]] = []
with mysql.connector.connect(**args_source_dsn) as con:
    cur = con.cursor()
    cur.execute(f"SELECT TABLE_SCHEMA, table_name FROM information_schema.tables WHERE TABLE_SCHEMA IN ({_dbs})")
    for db, tab in cur.fetchall():
        tables.append((db, tab))


def f(src_db: str, src_tab: str):
    try:
        MysqlTableCompare(args_source_dsn, args_target_dsn, src_db, src_tab, src_db, src_tab, 10, 200).run()
    except Exception as e:
        with open(f"{src_db}.{src_tab}.err.log", "w", encoding="utf8") as f:
            f.write(str(e))


if __name__ == "__main__":
    process_pool: dict[str, tuple[Process, str, str]] = {}

    for db, tab in tables:
        p = Process(target=f, args=(db, tab))
        process_pool[p.name] = (p, db, tab)

        p.start()

        while True:
            if len(process_pool) >= 3:
                time.sleep(1)
            else:
                break

            for pname, (prunning, db, tab) in process_pool.copy().items():
                if not prunning.is_alive():
                    prunning.join()
                    del process_pool[pname]
            print("running:", process_pool.values())
