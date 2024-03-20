import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

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
    cur.execute(f"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ({_dbs})")
    for db, tab in cur.fetchall():
        tables.append((db, tab))


def f(src_db: str, src_tab: str):
    for _ in range(0, 4):
        try:
            _s_ts = time.time()
            MysqlTableCompare(args_source_dsn, args_target_dsn, src_db, src_tab, src_db, src_tab, 8, 6000, 400).run()
            print(f"compare elapsed time: {src_db}.{src_tab} {round(time.time() - _s_ts, 2)}")
            return
        except Exception as e:
            with open(f"{src_db}.{src_tab}.err.log", "w", encoding="utf8") as f:
                f.write(str(e))


if __name__ == "__main__":
    compare_success = 0
    parallel = 4

    with ProcessPoolExecutor(max_workers=parallel) as executor:
        future_to_task = {executor.submit(f, src_db, src_tab): f"{src_db}.{src_tab}" for src_db, src_tab in tables}

        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                print(f"compare done: {task}")
                print(f"compare progress: {compare_success}/{len(tables)}")
            except Exception as e:
                print(f"{task} generated an exception: {e}")
            compare_success += 1

    print("compare all done.")
