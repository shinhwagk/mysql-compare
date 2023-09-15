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


# def get_row_statement(database, table, table_keys) -> str:
#     whereval = []
#     for coln, colt in table_keys:
#         if "int" in colt or "char" in colt or "date" in colt:
#             whereval.append(f"{coln} = %s")
#         else:
#             raise Exception(f"data type: {colt} not suppert yet.")
#     return f"SELECT * FROM {database}.{table} WHERE {' AND '.join(whereval)}"


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


class MysqlRepair:
    def __init__(self, source_dsn, target_dsn, database, table) -> None:
        self.source_con = connect(**source_dsn)
        self.source_con.time_zone = "+00:00"

        self.target_con = connect(**target_dsn)
        self.target_con.time_zone = "+00:00"
        self.repair_con = self.target_con

        self.repair_con.autocommit = False

        self.database = database
        self.table = table

        self.table_key = get_table_keys(self.source_con, database, table)

        self._val = None

    def run(self):
        if os.path.exists(f"{self.database}.{self.table}.diff.log"):
            with open(f"{self.database}.{self.table}.diff.log") as f:
                for i in f.readlines():
                    exec(f"self._val={i}")
                    self.__repair_row(self._val)
        self.__close()

    def __close(self):
        self.source_con.close()
        self.target_con.close()

    def __repair_row(self, diff_row: dict):
        source_row = get_table_row_by_key(self.source_con, self.database, self.table, self.table_key, diff_row)
        target_row = get_table_row_by_key(self.target_con, self.database, self.table, self.table_key, diff_row)

        if source_row == target_row:
            print("source row == target row")
            print("source_row", source_row)
            print("target_row", target_row)
            return

        if target_row != diff_row:
            print(source_row)
            print(target_row)
            print(diff_row)
            print("target row has changed.")
            return

        # query_row_statement = get_row_statement(self.database, self.table, self.table_key)
        # query_row_statement_for_update = f"{query_row_statement} for update"

        # self.repair_con.start_transaction(isolation_level="READ COMMITTED")
        # with self.repair_con.cursor(dictionary=True) as cur:
        #     cur.execute(query_row_statement_for_update)
        #     _row: dict = cur.fetchone()
        #     if _row is not None and _row == target_row:
        #         _params_statement = ",".join(["%s" for i in range(0, len(_row.keys()))])
        #         _exec_statement = f"REPLACE INTO {database}.{table} VALUES ({_params_statement})"
        #         print(_exec_statement, tuple(diff_row.values()))
        #         self.repair_con.rollback()
        #         # cur.execute(_exec_statement, tuple(row.values()))
        #         # repair_con.commit()

        #     else:
        #         print("diff", target_row, _row)
        #         self.repair_con.rollback()


import argparse

from mysql_compare.mysql_compare import MysqlTableCompare

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="ProgramName", description="What the program does", epilog="Text at the bottom of help")
    parser.add_argument("--source-dsn", type=str, required=True)
    parser.add_argument("--target-dsn", type=str, required=True)
    parser.add_argument("--database", type=str, required=True)
    parser.add_argument("--table", type=str, required=True)
    args = parser.parse_args()

    _userpass, _hostport = args.source_dsn.split("@")
    _user, _pass = _userpass.split("/")
    _host, _port = _hostport.split(":")
    _source_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

    _userpass, _hostport = args.target_dsn.split("@")
    _user, _pass = _userpass.split("/")
    _host, _port = _hostport.split(":")
    _target_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

    _database = args.database
    _table = args.table

    MysqlRepair(_source_dsn, _target_dsn, _database, _table).run()
