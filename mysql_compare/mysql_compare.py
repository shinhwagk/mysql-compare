import datetime
import itertools
import json
import logging
import math
import os
import queue
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


def get_table_rows_by_keys(con: MySQLConnection, database, table, table_keys: list[tuple[str, str]], table_rows: list[dict]) -> str:
    cols = []
    for coln, colt in table_keys:
        if "int" in colt or "double" in colt or "char" in colt or "date" == colt or "decimal" == colt:
            cols.append(coln)
        else:
            raise Exception(f"data type: [{colt}] not suppert yet.")

    _values = [list(d.values()) for d in table_rows]
    _params = ["(" + ", ".join("%s" for _ in _v) + ")" for _v in _values]

    _stmt = f"SELECT * FROM {database}.{table} WHERE ({', '.join(cols)}) IN ({', '.join(_params)}) "

    _values = tuple(itertools.chain.from_iterable(_values))

    with con.cursor(dictionary=True) as cur:
        cur.execute(_stmt, tuple(_values))
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


def get_elapsed_time(st: float, ndigits=None) -> int | float:
    return round(time.time() - st, ndigits)


def create_mysql_connection(dsn: dict) -> MySQLConnection:
    for i in range(0, 5):
        try:
            return connect(**dsn, time_zone="+00:00")
        except Exception as e:
            if i == 4:
                raise e
            print("try create mysql connection again.", dsn, e)
            time.sleep(5)


class MysqlTableCompare:
    def __init__(
        self,
        src_dsn: dict,
        dst_dsn: dict,
        src_database: str,
        src_table: str,
        dst_database: str,
        dst_table: str,
        parallel: int = 1,
        fetch_size: int = 6000,
        shard_size: int = 200,
    ) -> None:
        self.source_dsn = src_dsn
        self.target_dsn = dst_dsn

        self.parallel = parallel

        self.fetch_size = fetch_size
        self.shard_size = shard_size

        self.src_database = src_database
        self.src_table = src_table
        self.dst_database = dst_database
        self.dst_table = dst_table

        self.processed_rows_number = 0
        self.processed_diff_rows: list[dict] = []

        self.start_timestamp = time.time()

        self.compare_name = f"{self.src_database}.{self.src_table}.{self.dst_database}.{self.dst_table}"

        self.checkpoint_file = f"{self.compare_name}.ckpt.json"
        self.done_file = f"{self.compare_name}.done"

        if self.fetch_size % self.shard_size != 0:
            raise Exception(f"The remainder of the fetch_size divided by the shard_size must be equal to 0.")

    def result_worker(self, task_result_queue: queue.Queue):
        _batch_id = 0
        _tasks: list[tuple[int, int, list[dict], list[dict], float]] = []
        while True:
            task: tuple[int, int, list[dict], list[dict], float] = task_result_queue.get()
            if task is None:
                break

            _tasks.append(task)

            while True:
                matched = False
                for i in range(len(_tasks) - 1, -1, -1):

                    task_id, batch_id, rows, diff_rows, row_proc_etime = _tasks[i]

                    if batch_id == _batch_id:
                        self.processed_rows_number += len(rows)

                        if diff_rows:
                            self.write_diff_row(diff_rows)

                        self.processed_diff_rows += diff_rows

                        if self.processed_rows_number % 10000 == 0:
                            _l_row = rows[-1]
                            self.write_checkpoint(_l_row, self.processed_rows_number)
                            self.logger.debug(f"checkpoint:{_l_row}")

                        self.logger.debug(
                            f"threading[{task_id}], batch[{batch_id}], progress: {round(self.processed_rows_number/max(1, self.source_table_rows_number)*100, 1)}%, {self.processed_rows_number}/{max(1, self.source_table_rows_number)}, elapsed time:{row_proc_etime}s"
                        )

                        _batch_id += 1

                        del _tasks[i]

                        matched = True

                if not matched:
                    break

    def worker(
        self,
        task_id: int,
        task_queue: queue.Queue,
        task_result_queue: queue.Queue,
        task_error_event: threading.Event,
        task_error_queue: queue.Queue,
    ):
        _src_conn = create_mysql_connection(self.source_dsn)
        _dst_conn = create_mysql_connection(self.target_dsn)
        _err_try = 10

        while not task_error_event.is_set():
            task: tuple[int, list[dict]] = task_queue.get()
            if task is None:
                break

            batch_id, source_key_vals = task

            if source_key_vals:
                for try_cnt in range(1, _err_try + 1):
                    try:
                        _s_ts = time.time()
                        diff_rows = self.process_rows(task_id, _src_conn, _dst_conn, source_key_vals)
                        task_result_queue.put((task_id, batch_id, source_key_vals, diff_rows, get_elapsed_time(_s_ts, 2)))
                        break
                    except Exception as e:
                        self.logger.error(f"threading:[{task_id}] - {str(e)}.")
                        self.logger.error(f"threading:[{task_id}] - retry: {try_cnt}/{_err_try}")

                        if try_cnt == 10:
                            task_error_event.set()
                            task_error_queue.put(e)
                        else:
                            time.sleep(30)
                            try:
                                _src_conn = create_mysql_connection(self.source_dsn)
                                _dst_conn = create_mysql_connection(self.target_dsn)
                            except Exception as ce:
                                self.logger.error(f"threading:[{task_id}] - {str(ce)}.")

            task_queue.task_done()

    def get_full_table_orderby_keys(self, limit_size: int, keycols: list[tuple[str, str]], keyval: dict = None):
        _keyval = keyval
        # select * from where 1 = 1 and ((a > xxx) or (a = xxx and b > yyy) or (a = xxx and b = yyy and c > zzz)) order by a,b,c limit checksize
        params: list = []
        while True:
            key_colns = ", ".join(list(map(lambda c: c[0], keycols)))
            if _keyval is None:
                statement = f"SELECT {key_colns} FROM {self.src_database}.{self.src_table} ORDER BY {key_colns} limit {limit_size}"
            else:
                whereval = []
                for j in range(0, len(keycols)):
                    _kcs = keycols.copy()[0 : j + 1]
                    unit = []
                    for i in range(0, len(_kcs)):
                        coln, colt = _kcs[i]
                        soy = ">" if i == len(_kcs) - 1 else "="
                        if "int" in colt or "double" in colt or "char" in colt or "date" == colt or "decimal" == colt:
                            unit.append(f"{coln} {soy} %s")
                            params.append(_keyval[coln])
                        else:
                            raise Exception(f"data type: [{colt}] not suppert yet.")
                    whereval.append(" and ".join(unit))
                where = "(" + ") or (".join(whereval) + ")"
                statement = f"SELECT {key_colns} FROM {self.src_database}.{self.src_table} WHERE {where} ORDER BY {key_colns} limit {limit_size}"

            self.logger.debug(f"get_full_table_orderby_keys: {statement}, params {params}")

            with self.source_con.cursor(dictionary=True) as cur:
                if len(params) == 0:
                    cur.execute(statement)
                else:
                    cur.execute(statement, params=tuple(params.copy()))
                params.clear()
                for _ in range(0, int(limit_size / self.shard_size)):
                    rows = cur.fetchmany(size=self.shard_size)
                    if len(rows) == self.shard_size:
                        _keyval = rows[-1]
                        yield rows
                    elif len(rows) >= 1:
                        yield rows
                    else:
                        return

    def process_rows(
        self,
        task_id: int,
        source_con: MySQLConnection,
        target_con: MySQLConnection,
        source_key_vals: list[dict],
    ):
        _s_ts = time.time()
        _target_rows = get_table_rows_by_keys(target_con, self.dst_database, self.dst_table, self.source_table_keys, source_key_vals)
        self.logger.debug(f"threading:[{task_id}] - target rows query elapsed time {get_elapsed_time(_s_ts,2)}s, count: {len(_target_rows)}.")

        _s_ts = time.time()
        _source_rows = get_table_rows_by_keys(source_con, self.src_database, self.src_table, self.source_table_keys, source_key_vals)
        self.logger.debug(f"threading:[{task_id}] - source rows query elapsed time {get_elapsed_time(_s_ts,2)}s, count: {len(_source_rows)}.")

        return list(itertools.filterfalse(lambda x: x in _target_rows, _source_rows))

    def write_diff_row(self, rows: list[dict]):
        with open(f"{self.src_database}.{self.src_table}.diff.log", "a", encoding="utf8") as f:
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
        self.logger = init_logger(f"{self.src_database}.{self.src_table}.{self.dst_database}.{self.dst_table}")

        self.logger.info(f"start {self.compare_name}")
        if os.path.exists(self.done_file):
            print(f"compare {self.compare_name} done.")
            return

        self.source_con = create_mysql_connection(self.source_dsn)
        self.target_con = create_mysql_connection(self.target_dsn)

        source_table_struct = get_table_structure(self.source_con, self.src_database, self.src_table)
        target_table_struct = get_table_structure(self.target_con, self.dst_database, self.dst_table)

        self.logger.info(f"source table structure: {source_table_struct}")
        self.logger.info(f"target table structure: {target_table_struct}")

        table_struct_diff = list(itertools.filterfalse(lambda x: x in target_table_struct, source_table_struct))
        if not (len(source_table_struct) == len(target_table_struct) and len(table_struct_diff) == 0):
            raise Exception(f"source and target table structure diff.")

        self.logger.info(f"source and target table structure same.")

        self.source_table_keys = get_table_keys(self.source_con, self.src_database, self.src_table)
        self.logger.info(f"source table keys: {self.source_table_keys}.")

        self.source_table_rows_number = get_table_rows_number(self.source_con, self.src_database, self.src_table)
        self.target_table_rows_number = get_table_rows_number(self.target_con, self.dst_database, self.dst_table)

        self.logger.info(f"source table rows number: {self.source_table_rows_number}.")
        self.logger.info(f"target table rows number: {self.target_table_rows_number}.")
        # if self.source_table_rows_number == 0:
        #     self.logger.info("source table rows 0.")
        #     return

        # loop full table
        _checkpoint = self.read_checkpoint()
        _checkpoint_row = None

        if _checkpoint is not None:
            self.logger.info(f"checkpoint: {_checkpoint}")
            self.processed_rows_number = _checkpoint.processed
            _checkpoint_row = _checkpoint.checkpoint

        self.logger.info(f"from checkpoint: {_checkpoint}")

        _worker_threads: list[threading.Thread] = []

        _task_queue_length = math.ceil(self.parallel * 1.2)
        _task_queue = queue.Queue(maxsize=_task_queue_length)
        _task_result_queue = queue.Queue(maxsize=self.parallel)
        _task_error_queue = queue.Queue()
        _task_error_event = threading.Event()

        for t_id in range(_task_queue_length):
            t = threading.Thread(
                target=self.worker,
                args=(
                    t_id,
                    _task_queue,
                    _task_result_queue,
                    _task_error_event,
                    _task_error_queue,
                ),
            )
            t.start()
            _worker_threads.append(t)

        _worker_result_thread = threading.Thread(target=self.result_worker, args=(_task_result_queue,))
        _worker_result_thread.start()

        _batch_id = 0
        for rows in self.get_full_table_orderby_keys(self.fetch_size, self.source_table_keys, _checkpoint_row):
            _task_queue.put([_batch_id, rows])
            _batch_id += 1

        for _ in range(_task_queue_length):
            _task_queue.put(None)

        for t in _worker_threads:
            t.join()

        _task_result_queue.put(None)
        _worker_result_thread.join()

        if not _task_error_queue.empty():
            err = _task_error_queue.get()
            self.logger.error(f"task err {err}")
            raise err

        self.logger.info(f"compare completed, elapsed time:{get_elapsed_time(self.start_timestamp, 2)}.")

        with open(self.done_file, "w", encoding="utf8") as f:
            pass

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)


if __name__ == "__main__":
    MysqlTableCompare(
        {"host": "192.168.161.93", "port": 33061, "user": "root", "password": "my-secret-pw"},
        {"host": "192.168.161.93", "port": 33062, "user": "root", "password": "my-secret-pw"},
        "test1",
        "tab1",
        "test1",
        "tab1",
    ).run()
