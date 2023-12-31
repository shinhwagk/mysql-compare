import datetime
import os
from decimal import Decimal

from mysql.connector import MySQLConnection, connect


def get_table_keys(con: MySQLConnection, database, table) -> list[tuple[str, str]]:
    query_table_keys_statement = """
        SELECT
            tis.index_name,
            titc.constraint_type,
            tic.column_name,
            tic.data_type
        FROM information_schema.table_constraints titc,
            information_schema.statistics         tis,
            information_schema.columns            tic
        WHERE titc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        AND titc.table_schema    = %s
        AND titc.table_name      = %s
        AND titc.table_schema    = tis.table_schema
        AND titc.table_name      = tis.table_name
        AND titc.constraint_name = tis.index_name
        AND tis.table_schema     = tic.table_schema
        AND tis.table_name       = tic.table_name
        AND tis.column_name      = tic.column_name
    """

    with con.cursor() as cur:
        cur.execute(query_table_keys_statement, (database, table))
        rows = [row for row in cur.fetchall()]

    pkey = list(map(lambda c: (c[2], c[3]), filter(lambda c: c[1] == "PRIMARY KEY", rows)))

    if len(pkey) >= 1:
        return pkey

    ukeys = list(filter(lambda c: c[1] == "UNIQUE", rows))

    if len(ukeys) == 0:
        raise Exception(f"not have primary key or unique keys.")

    _first_unique = ukeys[0][0]

    ukey = list(
        map(
            lambda c: (c[2], c[3]),
            filter(lambda c: c[1] == "UNIQUE" and c[0] == _first_unique, rows),
        )
    )

    return ukey


def get_table_row_by_key(con: MySQLConnection, database, table, table_keys, diff_row) -> str:
    whereval = []
    params: list = []
    for coln, colt in table_keys:
        if "int" in colt or "char" in colt or "date" in colt:
            whereval.append(f"{coln} = %s")
            params.append(diff_row[coln])
        else:
            raise Exception(f"data type: {colt} not suppert yet.")
    _stmt = f"SELECT * FROM {database}.{table} WHERE {' AND '.join(whereval)}"

    with con.cursor(dictionary=True) as cur:
        cur.execute(_stmt, params=tuple(params))
        return cur.fetchone()


def compare(log_location, source_dsn, target_dsn, database, table):
    log_location = log_location

    source_con = connect(**source_dsn)
    source_con.time_zone = "+00:00"

    target_con = connect(**target_dsn)
    target_con.time_zone = "+00:00"

    database = database
    table = table

    table_key = get_table_keys(source_con, database, table)

    lcls = {}
    with open(f"{log_location}/{database}.{table}.diff.log") as f:
        for i in f.readlines():
            exec(f"_val={i}", globals(), lcls)
            _val = lcls["_val"]
            source_row = get_table_row_by_key(source_con, database, table, table_key, _val)
            target_row = get_table_row_by_key(target_con, database, table, table_key, _val)
            if target_row != target_row:
                print(source_row)
                print(target_row)
                print("target row has changed.")
                return
            else:
                print(f"row pass {_val}.")
    source_con.close()
    target_con.close()


if __name__ == "__main__":
    ARGS_SOURCE_DSN = os.environ.get("ARGS_SOURCE_DSN")
    ARGS_TARGET_DSN = os.environ.get("ARGS_TARGET_DSN")
    ARGS_LOG_LOCATION = os.environ.get("ARGS_LOG_LOCATION")

    _userpass, _hostport = ARGS_SOURCE_DSN.split("@")
    _user, _pass = _userpass.split("/")
    _host, _port = _hostport.split(":")
    _source_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

    _userpass, _hostport = ARGS_TARGET_DSN.split("@")
    _user, _pass = _userpass.split("/")
    _host, _port = _hostport.split(":")
    _target_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

    _log_location = ARGS_LOG_LOCATION

    for f in [f for f in os.listdir(_log_location) if os.path.isfile(f)]:
        if f.endswith(".diff.log"):
            _f = f.split(".")
            _database = _f[0]
            _table = _f[1]
            compare(_log_location, _source_dsn, _target_dsn, _database, _table)
