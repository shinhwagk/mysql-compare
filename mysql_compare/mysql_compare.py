import datetime
import itertools
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from decimal import Decimal

from mysql.connector import MySQLConnection, connect


@dataclass
class Checkpoint:
    checkpoint: dict
    processed: int


def init_logger(name: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(f"{name}.log")
    FILE_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_handler.setFormatter(logging.Formatter(FILE_LOG_FORMAT))
    logger.addHandler(file_handler)
    return logger


def get_table_rows_number(con: MySQLConnection, database, table):
    with con.cursor() as cur:
        cur.execute("SELECT table_rows FROM information_schema.tables WHERE table_schema = %s AND table_name = %s", (database, table))
        (rows,) = cur.fetchone()
        return rows


def get_table_structure(con: MySQLConnection, database, table) -> list[tuple[str, str]]:
    with con.cursor() as cur:
        cur.execute(
            "SELECT column_name, CAST(data_type as char(255)) FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY 1",
            (database, table),
        )
        return [row for row in cur.fetchall()]


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


def get_table_rows_by_keys(con: MySQLConnection, database, table, table_keys: list[tuple[str, str]], table_keys_vals: list[dict]) -> str:
    whereval = []
    for coln, colt in table_keys:
        if "int" in colt or "char" in colt or "date" == colt or "decimal" == colt:
            whereval.append(f"{coln} = %s")
        else:
            raise Exception(f"data type: [{colt}] not suppert yet.")

    _s_stmt = f"SELECT * FROM {database}.{table} WHERE {' AND '.join(whereval)}"

    _stmt = " UNION ALL ".join([_s_stmt for _ in range(0, len(table_keys_vals))])

    _values = list(map(lambda r: r.values(), table_keys_vals))
    _params = [item for t in _values for item in t]

    with con.cursor(dictionary=True) as cur:
        cur.execute(_stmt, tuple(_params))
        return [row for row in cur.fetchall()]


def get_full_table(con: MySQLConnection, database, table, cols: list[str]):
    key_colns = ", ".join(cols)
    statement = f"SELECT {key_colns} FROM {database}.{table}"

    with con.cursor(dictionary=True) as cur:
        cur.execute(statement)
        while True:
            row = cur.fetchone()
            if row is None:
                break
            yield row


def get_elapsed_time(st: float, ndigits=None):
    return round(time.time() - st, ndigits)


def create_mysql_connection(dsn: dict) -> MySQLConnection:
    for i in range(0, 5):
        try:
            return connect(**dsn, time_zone="+00:00")
        except Exception as e:
            if i == 4:
                raise e
            print("create_mysql_connection", dsn, e)
            time.sleep(5)


class MysqlTableCompare:
    def __init__(
        self,
        source_dsn: dict,
        target_dsn: dict,
        database: str,
        table: str,
        parallel: int = 1,
        fetch_size: int = 100,
    ) -> None:
        self.source_dsn = source_dsn
        self.target_dsn = target_dsn

        self.parallel = parallel

        self.fetch_size = fetch_size

        self.database = database
        self.table = table

        self.processed_rows_number = 0

        self.start_timestamp = time.time()

        self.checkpoint_file = f"{self.database}.{self.table}.ckpt.json"

    def get_full_table_orderby_keys(self, limit_size: int, keycols: list[tuple[str, str]], keyval: dict = None):
        _keyval = keyval
        # select * from where 1 = 1 and ((a > xxx) or (a = xxx and b > yyy) or (a = xxx and b = yyy and c > zzz)) order by a,b,c limit checksize
        params: list = []
        while True:
            key_colns = ", ".join(list(map(lambda c: c[0], keycols)))
            if _keyval is None:
                statement = f"SELECT {key_colns} FROM {self.database}.{self.table} ORDER BY {key_colns} limit {limit_size}"
            else:
                whereval = []
                for j in range(0, len(keycols)):
                    _kcs = keycols.copy()[0 : j + 1]
                    unit = []
                    for i in range(0, len(_kcs)):
                        coln, colt = _kcs[i]
                        soy = ">" if i == len(_kcs) - 1 else "="
                        if "int" in colt or "char" in colt or "date" == colt or "decimal" == colt:
                            unit.append(f"{coln} {soy} %s")
                            params.append(_keyval[coln])
                        else:
                            raise Exception(f"data type: [{colt}] not suppert yet.")
                    whereval.append(" and ".join(unit))
                where = "(" + ") or (".join(whereval) + ")"
                statement = f"SELECT {key_colns} FROM {self.database}.{self.table} WHERE {where} ORDER BY {key_colns} limit {limit_size}"

            self.logger.debug(f"get_full_table_orderby_keys: {statement}, params {params}")

            with self.source_con.cursor(dictionary=True) as cur:
                if len(params) == 0:
                    cur.execute(statement)
                else:
                    cur.execute(statement, params=tuple(params.copy()))
                params.clear()
                for _ in range(0, int(limit_size / 200)):
                    rows = cur.fetchmany(size=200)
                    if len(rows) == 200:
                        _keyval = rows[-1]
                        yield False, rows
                    elif len(rows) >= 1:
                        yield True, rows
                    else:
                        return

    def process_rows(
        self,
        process_id: int,
        source_con: MySQLConnection,
        target_con: MySQLConnection,
        source_key_vals: list[dict],
        diff_rows: list[dict],
        try_cnt: int = 0,
    ):
        try:
            _s_ts = time.time()
            _target_rows = get_table_rows_by_keys(target_con, self.database, self.table, self.source_table_keys, source_key_vals)
            self.logger.debug(f"threading running:[{process_id}] - target rows query elapsed time {get_elapsed_time(_s_ts,2)}s, count: {len(_target_rows)}.")

            _s_ts = time.time()
            _source_rows = get_table_rows_by_keys(source_con, self.database, self.table, self.source_table_keys, source_key_vals)
            self.logger.debug(f"threading running:[{process_id}] - source rows query elapsed time {get_elapsed_time(_s_ts,2)}s, count: {len(_source_rows)}.")

            _diff_rows = list(itertools.filterfalse(lambda x: x in _target_rows, _source_rows))
            diff_rows += _diff_rows

        except Exception as e:
            self.logger.error(f"threading running:[{process_id}] - {str(e)}")
            if try_cnt >= 5:
                self.logger.error(f"threading running:[{process_id}] - retry: {try_cnt}/5")
                raise e
            else:
                time.sleep(30)
                self.process_rows(process_id, source_con, target_con, source_key_vals, try_cnt + 1)

    def write_diff_row(self, rows: list[dict]):
        with open(f"{self.database}.{self.table}.diff.log", "a", encoding="utf8") as f:
            for row in rows:
                f.write(f"{row}\n")

    def write_checkpoint(self, row: dict, processed: int):
        with open(self.checkpoint_file, "w", encoding="utf8") as f:
            json.dump({"checkpoint": row, "processed": processed}, f, default=str)

    def read_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r", encoding="utf8") as f:
                ckpt: Checkpoint = Checkpoint(**json.load(f))
            for k, v in ckpt.checkpoint.items():
                for coln, colt in self.source_table_keys:
                    if k == coln:
                        if colt == "date":
                            ckpt.checkpoint[k] = datetime.datetime.strptime(v, "%Y-%m-%d")
                        elif colt == "decimal":
                            ckpt.checkpoint[k] = Decimal(v)

            return ckpt
        return None

    def run(self) -> None:
        self.logger = init_logger(f"{self.database}.{self.table}")

        self.logger.info(f"start {self.database}{self.table}")
        if os.path.exists(f"{self.database}.{self.table}.done"):
            print(f"compare {self.database}{self.table} done")
            return

        self.source_con = create_mysql_connection(self.source_dsn)
        self.target_con = create_mysql_connection(self.target_dsn)

        source_table_struct = get_table_structure(self.source_con, self.database, self.table)
        target_table_struct = get_table_structure(self.target_con, self.database, self.table)

        self.logger.info(f"source table structure: {source_table_struct}")
        self.logger.info(f"target table structure: {target_table_struct}")

        table_struct_diff = list(itertools.filterfalse(lambda x: x in target_table_struct, source_table_struct))
        if not (len(source_table_struct) == len(target_table_struct) and len(table_struct_diff) == 0):
            raise Exception(f"source and target table structure diff.")

        self.logger.info(f"source and target table structure same.")

        self.source_table_keys = get_table_keys(self.source_con, self.database, self.table)
        self.logger.info(f"source table keys: {self.source_table_keys}.")

        self.source_table_rows_number = get_table_rows_number(self.source_con, self.database, self.table)
        self.target_table_rows_number = get_table_rows_number(self.target_con, self.database, self.table)

        self.logger.info(f"source table rows number: {self.source_table_rows_number}.")
        self.logger.info(f"target table rows number: {self.target_table_rows_number}.")
        if self.source_table_rows_number == 0:
            self.logger.info("source table rows 0.")
            return

        _thread_container: list[tuple[int, threading.Thread, MySQLConnection, MySQLConnection, list[dict], int, float, list[dict]]] = []
        _compare_conns = [(i, create_mysql_connection(self.source_dsn), create_mysql_connection(self.target_dsn), 0) for i in range(self.parallel)]

        # loop full table
        _checkpoint = self.read_checkpoint()
        _checkpoint_row = None

        if _checkpoint is not None:
            self.logger.info(f"checkpoint: {_checkpoint}")
            self.processed_rows_number = _checkpoint.processed
            _checkpoint_row = _checkpoint.checkpoint

        self.logger.info(f"from checkpoint: {_checkpoint}")

        for end, rows in self.get_full_table_orderby_keys(6000, self.source_table_keys, _checkpoint_row):
            _i, _sc, _tc, _cnt = _compare_conns.pop(0)

            _d_rows = []

            _t = threading.Thread(target=self.process_rows, args=(_i, _sc, _tc, rows.copy(), _d_rows))

            _thread_container.append((_i, _t, _sc, _tc, rows.copy(), _cnt + 1, time.time(), _d_rows))
            _t.start()

            while (len(_thread_container) == self.parallel) or (end and len(_thread_container) >= 1):
                _i, _t, _sc, _tc, _rows, _cnt, _s_ts, _d_rows = _thread_container.pop(0)
                _t.join()

                self.processed_rows_number += len(_rows)

                self.logger.debug(
                    f"threading release[{_i}], progress: {round(self.processed_rows_number/self.source_table_rows_number *100, 1)}%, {self.processed_rows_number}/{self.source_table_rows_number}, elapsed time:{get_elapsed_time(_s_ts, 2)}s"
                )

                if len(_d_rows) >= 1:
                    self.logger.info(f"discover diff repair rows: {len(_d_rows)}.")
                    self.write_diff_row(_d_rows)

                if _cnt >= 100:
                    for _c in [_sc, _tc]:
                        try:
                            _c.close()
                        except Exception as e:
                            self.logger.error(f"close {_c.connection_id}, {e}")
                    _sc = create_mysql_connection(self.source_dsn)
                    _tc = create_mysql_connection(self.target_dsn)
                    _cnt = 0

                if self.processed_rows_number % 10000 == 0:
                    _l_row = _rows[-1]
                    self.write_checkpoint(_l_row, self.processed_rows_number)
                    self.logger.debug(f"checkpoint:{_l_row}")

                _compare_conns.append((_i, _sc, _tc, _cnt))
                time.sleep(0.01)

        self.logger.info(f"compare completed, elapsed time:{get_elapsed_time(self.start_timestamp, 2)}.")

        with open(f"{self.database}.{self.table}.done", "w", encoding="utf8") as f:
            pass

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
