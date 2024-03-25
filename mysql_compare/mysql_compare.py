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
    file_handler.setLevel(logging.DEBUG)

    file_handler_error = logging.FileHandler(f"{name}.err.log")
    file_handler_error.setLevel(logging.ERROR)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    file_handler_error.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(file_handler_error)

    return logger


def get_table_rows_number(con: MySQLConnection, database: str, table: str) -> int:
    with con.cursor() as cur:
        cur.execute("SELECT table_rows FROM information_schema.tables WHERE table_schema = %s AND table_name = %s", (database, table))
        (rows,) = cur.fetchone()
        return rows


def get_table_structure(con: MySQLConnection, database: str, table: str) -> list[tuple[str, str]]:
    with con.cursor() as cur:
        cur.execute(
            "SELECT column_name, CAST(data_type as char(255)) FROM information_schema.columns WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (database, table),
        )
        return cur.fetchall()


def get_table_keys(con: MySQLConnection, database: str, table: str) -> list[tuple[str, str]]:
    query_table_keys_statement = """
        SELECT 
            tis.index_name, 
            titc.constraint_type, 
            tic.column_name, 
            tic.data_type
        FROM information_schema.table_constraints titc
        JOIN information_schema.statistics tis ON titc.table_schema = tis.table_schema AND titc.table_name = tis.table_name AND titc.constraint_name = tis.index_name
        JOIN information_schema.columns tic ON tis.table_schema = tic.table_schema AND tis.table_name = tic.table_name AND tis.column_name = tic.column_name
        WHERE titc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        AND titc.table_schema = %s
        AND titc.table_name = %s
    """

    with con.cursor() as cur:
        cur.execute(query_table_keys_statement, (database, table))
        rows = cur.fetchall()

    keys = [(row[2], row[3]) for row in rows if row[1] == "PRIMARY KEY"] or [(row[2], row[3]) for row in rows if row[1] == "UNIQUE" and row[0] == rows[0][0]]

    if not keys:
        raise Exception(f"{table} does not have primary key or unique keys.")

    return keys


def get_table_rows_by_keys(con: MySQLConnection, database: str, table: str, table_keys: list[tuple[str, str]], table_keys_rows: list[dict]) -> str:
    cols = [f"`{key[0]}`" for key in table_keys]
    placeholders = ", ".join(["%s"] * len(cols))

    in_clause = ", ".join([f"({placeholders})" for _ in table_keys_rows])

    params = [val for row in table_keys_rows for val in (row[col] for col in cols)]

    _stmt = f"SELECT * FROM {database}.{table} WHERE ({', '.join(cols)}) IN ({in_clause})"

    with con.cursor(dictionary=True) as cur:
        cur.execute(_stmt, tuple(params))
        return cur.fetchall()


def get_elapsed_time(st: float, ndigits=None) -> int | float:
    return round(time.time() - st, ndigits)


def recreate_connection(dsn: dict, conn: MySQLConnection | None = None) -> MySQLConnection:
    if conn:
        try:
            conn.close()
        except:
            pass
    for _ in range(5):
        try:
            return connect(**dsn, time_zone="+00:00")
        except Exception:
            time.sleep(5)
    raise Exception("Failed to create MySQL connection after 5 attempts.")


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
        fetch_size: int = 2000,
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

        self.start_timestamp = time.time()

        self.compare_name = f"{self.src_database}.{self.src_table}.{self.dst_database}.{self.dst_table}"

        self.checkpoint_file = f"{self.compare_name}.ckpt.json"
        self.done_file = f"{self.compare_name}.done"

        if self.fetch_size % self.shard_size != 0:
            raise Exception(f"The remainder of the fetch_size divided by the shard_size must be equal to 0.")

    def result_worker(self, task_result_queue: queue.Queue):
        _thread_ident = threading.current_thread().name
        _batch_id = 0
        _tasks: list[tuple[int, list[dict], list[dict], float]] = []

        while True:
            _task = task_result_queue.get()

            if _task is None:
                break

            _tasks.append(_task)
            _tasks.sort(key=lambda x: x[0])

            while _tasks and _tasks[0][0] == _batch_id:
                batch_id, rows, diff_rows, row_proc_etime = _tasks.pop(0)

                self.processed_rows_number += len(rows)

                if diff_rows:
                    self.write_diff_row(diff_rows)

                if self.processed_rows_number % 10000 == 0:
                    _l_row = rows[-1]
                    self.write_checkpoint(_l_row, self.processed_rows_number)
                    self.logger.debug(f"checkpoint:{_l_row}")

                self.logger.debug(
                    f"threading[{_thread_ident}], batch[{batch_id}], progress: {round(self.processed_rows_number/max(1, self.source_table_rows_number)*100, 1)}%, {self.processed_rows_number}/{self.source_table_rows_number}, elapsed time:{row_proc_etime}s"
                )

                _batch_id += 1

                task_result_queue.task_done()

        task_result_queue.task_done()

    def worker(
        self,
        task_queue: queue.Queue,
        task_result_queue: queue.Queue,
        task_error_event: threading.Event,
        task_error_queue: queue.Queue,
    ):
        _thread_ident = threading.current_thread().name
        _src_conn = recreate_connection(self.source_dsn)
        _dst_conn = recreate_connection(self.target_dsn)
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
                        diff_rows = self.process_rows(_src_conn, _dst_conn, source_key_vals)
                        task_result_queue.put((batch_id, source_key_vals, diff_rows, get_elapsed_time(_s_ts, 2)))
                        break
                    except Exception as e:
                        self.logger.error(f"threading:[{_thread_ident}] - {str(e)}.")
                        self.logger.error(f"threading:[{_thread_ident}] - retry: {try_cnt}/{_err_try}")

                        if try_cnt == 10:
                            task_error_event.set()
                            task_error_queue.put(e)
                            self.logger.error(f"threading:[{_thread_ident}] - exit.")
                        else:
                            time.sleep(1)
                            try:
                                _src_conn = recreate_connection(self.source_dsn, _src_conn)
                                _dst_conn = recreate_connection(self.target_dsn, _dst_conn)
                            except Exception as ce:
                                self.logger.error(f"threading:[{_thread_ident}] - {str(ce)}.")

            task_queue.task_done()

        task_queue.task_done()

        for c in [_src_conn, _dst_conn]:
            try:
                c.close()
            except Exception:
                pass

    def get_full_table_orderby_keys(self, source_con: MySQLConnection, limit_size: int, keycols: list[tuple[str, str]], keyval: dict = None):
        _keyval = keyval
        # select * from where 1 = 1 and ((a > xxx) or (a = xxx and b > yyy) or (a = xxx and b = yyy and c > zzz)) order by a,b,c limit checksize
        params: list = []
        _key_colns = ", ".join([f"`{col[0]}`" for col in keycols])

        while True:
            if _keyval is None:
                statement = f"SELECT {_key_colns} FROM {self.src_database}.{self.src_table} ORDER BY {_key_colns} limit {limit_size}"
            else:
                whereval = []
                for j in range(0, len(keycols)):
                    _kcs = keycols[: j + 1]
                    unit = []
                    for i, (coln, colt) in enumerate(_kcs):
                        soy = ">" if i == len(_kcs) - 1 else "="
                        if "int" in colt or "double" in colt or "char" in colt or "date" == colt or "decimal" == colt:
                            unit.append(f"`{coln}` {soy} %s")
                            params.append(_keyval[coln])
                        else:
                            raise Exception(f"data type: [{colt}] not suppert yet.")
                    whereval.append(" and ".join(unit))
                where = "(" + ") or (".join(whereval) + ")"
                statement = f"SELECT {_key_colns} FROM {self.src_database}.{self.src_table} WHERE {where} ORDER BY {_key_colns} limit {limit_size}"

            self.logger.debug(f"get_full_table_orderby_keys: {statement}, params {params}")

            _has_data = False

            with source_con.cursor(dictionary=True) as cur:
                if len(params) == 0:
                    cur.execute(statement)
                else:
                    cur.execute(statement, params=tuple(params.copy()))

                params.clear()

                while True:
                    rows = cur.fetchmany(size=self.shard_size)

                    if rows:
                        _keyval = rows[-1]
                        _has_data = True
                        yield rows
                    else:
                        break

            if not _has_data:
                return

    def process_rows(
        self,
        source_con: MySQLConnection,
        target_con: MySQLConnection,
        source_key_vals: list[dict],
    ):
        _thread_ident = threading.current_thread().name

        _s_ts = time.time()
        _target_rows = get_table_rows_by_keys(target_con, self.dst_database, self.dst_table, self.source_table_keys, source_key_vals)
        self.logger.debug(f"threading:[{_thread_ident}] - target rows query elapsed time {get_elapsed_time(_s_ts,2)}s, count: {len(_target_rows)}.")

        _s_ts = time.time()
        _source_rows = get_table_rows_by_keys(source_con, self.src_database, self.src_table, self.source_table_keys, source_key_vals)
        self.logger.debug(f"threading:[{_thread_ident}] - source rows query elapsed time {get_elapsed_time(_s_ts,2)}s, count: {len(_source_rows)}.")

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

        if os.path.exists(self.done_file):
            return

        self.logger.info(f"start {self.compare_name}.")

        source_con = recreate_connection(self.source_dsn)
        target_con = recreate_connection(self.target_dsn)

        source_table_struct = get_table_structure(source_con, self.src_database, self.src_table)
        target_table_struct = get_table_structure(target_con, self.dst_database, self.dst_table)

        self.logger.info(f"source table structure: {source_table_struct}")
        self.logger.info(f"target table structure: {target_table_struct}")

        table_struct_diff = list(itertools.filterfalse(lambda x: x in target_table_struct, source_table_struct))
        if not (len(source_table_struct) == len(target_table_struct) and len(table_struct_diff) == 0):
            with open(self.done_file, "w", encoding="utf8") as f:
                pass
            self.logger.error(f"source and target table structure diff.")
            return

        self.logger.info(f"source and target table structure same.")

        try:
            self.source_table_keys = get_table_keys(source_con, self.src_database, self.src_table)
        except Exception as e:
            self.logger.error(str(e))
            return

        self.logger.info(f"source table keys: {self.source_table_keys}.")

        self.source_table_rows_number = max(1, get_table_rows_number(source_con, self.src_database, self.src_table))
        self.target_table_rows_number = max(1, get_table_rows_number(target_con, self.dst_database, self.dst_table))

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

        for _ in range(_task_queue_length):
            t = threading.Thread(
                target=self.worker,
                args=(
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
        _debug_fetch_rows = 0
        try:
            for rows in self.get_full_table_orderby_keys(
                source_con,
                self.fetch_size,
                self.source_table_keys,
                _checkpoint_row,
            ):
                _debug_fetch_rows += len(rows)
                _task_queue.put([_batch_id, rows])
                _batch_id += 1
        except Exception as e:
            self.logger.error(f"query table faile {str(e)}")

        for _ in range(_task_queue_length):
            _task_queue.put(None)

        _task_queue.join()
        self.logger.debug(f"queue:[task_queue] - release.")

        for t in _worker_threads:
            t.join()
            self.logger.debug(f"threading:[worker][{t.ident}] - release.")

        _task_result_queue.put(None)
        _task_result_queue.join()
        self.logger.debug(f"queue:[task_result_queue] - release.")

        _worker_result_thread.join()
        self.logger.debug(f"threading:[worker_result][{t.ident}] - release.")

        if not _task_error_queue.empty():
            err = _task_error_queue.get()
            self.logger.error(f"compare failed, {str(err)}.")
            return

        self.logger.info(f"compare completed, elapsed time:{get_elapsed_time(self.start_timestamp, 2)}s.")

        with open(self.done_file, "w", encoding="utf8") as f:
            pass

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)

        for c in [source_con, target_con]:
            try:
                c.close()
            except Exception:
                pass
