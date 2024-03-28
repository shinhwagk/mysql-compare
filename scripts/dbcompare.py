import os
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, as_completed, wait
from datetime import datetime

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
    cur.execute(f"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ({_dbs}) order by 1, 2")
    for db, tab in cur.fetchall():
        tables.append((db, tab))


def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _f(src_db: str, src_tab: str):
    if os.path.exists(f"{src_db}.{src_tab}.err.log"):
        os.remove(f"{src_db}.{src_tab}.err.log")

    _s_ts = time.time()
    print(f"{get_current_datetime()} compare start: {src_db}.{src_tab}.")
    for i in range(1, 6):
        try:
            MysqlTableCompare(args_source_dsn, args_target_dsn, src_db, src_tab, src_db, src_tab, 6, 6000, 400).run()
            print(f"{get_current_datetime()} compare done; elapsed time: {src_db}.{src_tab} {round(time.time() - _s_ts, 2)}s.")
            break
        except Exception as e:
            if i == 5:
                with open(f"{src_db}.{src_tab}.err.log", "w", encoding="utf8") as f:
                    f.write(str(e))


if __name__ == "__main__":
    compare_success = 0
    parallel = 6

    with ProcessPoolExecutor(max_workers=parallel) as executor:
        future_to_task = {executor.submit(_f, src_db, src_tab): f"{src_db}.{src_tab}" for src_db, src_tab in tables}

        for future in as_completed(future_to_task):
            task = future_to_task[future]
            compare_success += 1
            try:
                result = future.result()
            except Exception as e:
                print(f"{get_current_datetime()} {task} generated an exception: {e}")
            finally:
                print(f"{get_current_datetime()} compare progress: {compare_success}/{len(tables)}")

    print(f"{get_current_datetime()} compare all done.")
