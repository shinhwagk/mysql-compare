import datetime
import itertools
import json
import logging
import math
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal

from mysql.connector import MySQLConnection, connect


@dataclass
class Checkpoint:
    checkpoint: dict
    processed: int
    different: int


def init_logger(name: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    log_file_configs = [
        {"filename": f"{name}.log", "level": logging.DEBUG},
        {"filename": f"{name}.err.log", "level": logging.ERROR},
    ]

    for config in log_file_configs:
        handler = logging.FileHandler(config["filename"])
        handler.setLevel(config["level"])
        handler.setFormatter(formatter)
        logger.addHandler(handler)

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
        raise Exception(f"does not have primary key or unique keys.")

    return keys


def get_table_rows_by_keys(con: MySQLConnection, database: str, table: str, table_keys: list[tuple[str, str]], table_keys_rows: list[dict]) -> list[dict]:
    cols = [key[0] for key in table_keys]
    placeholders = ", ".join(["%s"] * len(cols))

    in_clause = ", ".join([f"({placeholders})" for _ in table_keys_rows])

    params = [val for row in table_keys_rows for val in (row[col] for col in cols)]

    formatted_cols = [f"`{c}`" for c in cols]
    _stmt = f"SELECT * FROM {database}.{table} WHERE ({', '.join(formatted_cols)}) IN ({in_clause})"

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
        self.different_rows_number = 0

        self.start_timestamp = time.time()

        self.compare_name: str = f"{self.src_database}.{self.src_table}"
        if self.src_database == self.dst_database and self.src_table != self.dst_table:
            self.compare_name += f".{self.dst_table}"
        elif self.src_database != self.dst_database:
            self.compare_name += f".{self.dst_database}.{self.dst_table}"

        self.checkpoint_file = f"{self.compare_name}.ckpt.json"
        self.done_file = f"{self.compare_name}.done"
        self.different_file = f"{self.compare_name}.diff.log"

        if self.fetch_size % self.shard_size != 0:
            raise Exception(f"The remainder of the fetch_size divided by the shard_size must be equal to 0.")

    def result_worker(self, task_result_queue: queue.Queue):
        _thread_ident = threading.current_thread().name
        _batch_id = 0
        _tasks: list[tuple[int, list[dict], list[dict]]] = []

        while True:
            _task = task_result_queue.get()

            if _task is None:
                task_result_queue.task_done()
                break

            _tasks.append(_task)
            _tasks.sort(key=lambda x: x[0])

            while _tasks and _tasks[0][0] == _batch_id:
                batch_id, source_key_vals, diff_rows = _tasks.pop(0)

                self.processed_rows_number += len(source_key_vals)

                if diff_rows:
                    self.write_diff_rows(diff_rows)
                    self.different_rows_number += len(diff_rows)

                if self.processed_rows_number % 10000 == 0:
                    self.write_checkpoint(source_key_vals[-1], self.processed_rows_number, self.different_rows_number)
                    self.logger.debug(f"checkpoint: {source_key_vals[-1]}")

                _progress_rate = round(self.processed_rows_number / max(1, self.source_table_rows_number) * 100, 1)
                self.logger.debug(f"threading[{_thread_ident}], batch[{batch_id}] - progress: {_progress_rate}%, total rows: {self.source_table_rows_number}.")
                self.logger.debug(f"threading[{_thread_ident}], batch[{batch_id}] - different: {self.different_rows_number}.")

                _batch_id += 1

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
                task_queue.task_done()
                break

            batch_id, source_key_vals = task

            if source_key_vals:
                for try_cnt in range(1, _err_try + 1):
                    try:
                        diff_rows = self.process_rows(batch_id, _src_conn, _dst_conn, source_key_vals)
                        task_result_queue.put((batch_id, source_key_vals, diff_rows))
                        break
                    except Exception as e:
                        self.logger.error(f"threading:[{_thread_ident}], batch[{batch_id}] - {str(e)}.")
                        self.logger.error(f"threading:[{_thread_ident}], batch[{batch_id}] - retry: {try_cnt}/{_err_try}")

                        if try_cnt == _err_try:
                            task_error_event.set()
                            task_error_queue.put(e)
                            self.logger.error(f"threading:[{_thread_ident}], batch[{batch_id}] - exit.")
                        else:
                            time.sleep(1)
                            try:
                                _src_conn = recreate_connection(self.source_dsn, _src_conn)
                                _dst_conn = recreate_connection(self.target_dsn, _dst_conn)
                            except Exception as ce:
                                self.logger.error(f"threading:[{_thread_ident}], batch[{batch_id}] - {str(ce)}.")

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

            self.logger.debug(f"threading:[{threading.current_thread().name}] - get_full_table_orderby_keys: {statement}, params {params}")

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
        batch_id: int,
        source_con: MySQLConnection,
        target_con: MySQLConnection,
        source_key_vals: list[dict],
    ):
        _thread_ident = threading.current_thread().name

        self.logger.debug(f"threading:[{_thread_ident}], batch[{batch_id}] - start compare rows {len(source_key_vals)}.")

        tasks = {
            "source": (source_con, self.src_database, self.src_table, self.source_table_keys, source_key_vals),
            "target": (target_con, self.dst_database, self.dst_table, self.source_table_keys, source_key_vals),
        }

        fetch_rows = lambda db_con, db_name, table_name, table_keys, key_vals: get_table_rows_by_keys(db_con, db_name, table_name, table_keys, key_vals)

        _s_ts = time.time()

        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_task = {executor.submit(fetch_rows, *tasks[task]): task for task in tasks}
            tasks = {key: [] for key in tasks}
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    tasks[task] = future.result()
                except Exception as e:
                    raise Exception(f"task[{task}] - fetch rows error: {str(e)}") from e

        self.logger.debug(
            f"threading:[{_thread_ident}], batch[{batch_id}] - compare rows elapsed time {get_elapsed_time(_s_ts,2)}s, count: {len(tasks[task])}."
        )

        return list(itertools.filterfalse(lambda x: x in tasks["target"], tasks["source"]))

    def write_diff_rows(self, rows: list[dict]):
        with open(self.different_file, "a", encoding="utf8") as f:
            for row in rows:
                f.write(f"{row}\n")

    def write_checkpoint(self, row: dict, processed: int, different: int):
        with open(self.checkpoint_file, "w", encoding="utf8") as f:
            json.dump({"checkpoint": row, "processed": processed, "different": different}, f, default=str)

    def read_checkpoint(self):
        if not os.path.exists(self.checkpoint_file):
            return None

        with open(self.checkpoint_file, "r", encoding="utf8") as f:
            ckpt: Checkpoint = Checkpoint(**json.load(f))

        for coln, colt in self.source_table_keys:
            if coln in ckpt.checkpoint:
                if colt == "date":
                    ckpt.checkpoint[coln] = datetime.datetime.strptime(ckpt.checkpoint[coln], "%Y-%m-%d")
                elif colt == "decimal":
                    ckpt.checkpoint[coln] = Decimal(ckpt.checkpoint[coln])
        return ckpt

    def run(self) -> None:
        self.logger = init_logger(self.compare_name)

        if os.path.exists(self.done_file):
            return

        self.logger.info(f"start {self.compare_name}.")

        try:
            with recreate_connection(self.source_dsn) as source_con, recreate_connection(self.target_dsn) as target_con:
                source_table_struct: list[tuple[str, str]] = get_table_structure(source_con, self.src_database, self.src_table)
                target_table_struct: list[tuple[str, str]] = get_table_structure(target_con, self.dst_database, self.dst_table)

                self.logger.info(f"source table structure: {source_table_struct}.")
                self.logger.info(f"target table structure: {target_table_struct}.")

                table_struct_diff = set(source_table_struct) - set(target_table_struct)
                if not source_table_struct or table_struct_diff:
                    raise Exception(f"source and target table structure diff.")

                self.logger.info(f"source and target table structure same.")

                self.source_table_keys = get_table_keys(source_con, self.src_database, self.src_table)

                self.logger.info(f"source table keys: {self.source_table_keys}.")

                self.source_table_rows_number = max(1, get_table_rows_number(source_con, self.src_database, self.src_table))
                self.logger.info(f"source table rows number: {self.source_table_rows_number}.")
        except Exception as e:
            self.logger.error(str(e))
            return

        _checkpoint = self.read_checkpoint()
        _checkpoint_row = None

        if _checkpoint:
            self.logger.info(f"checkpoint: {_checkpoint}")
            self.processed_rows_number = _checkpoint.processed
            self.different_rows_number = _checkpoint.different
            _checkpoint_row = _checkpoint.checkpoint

        self.logger.info(f"from checkpoint: {_checkpoint}")

        _worker_threads: list[threading.Thread] = []

        _task_queue_length = math.ceil(self.parallel * 1.2)
        _task_queue = queue.Queue(maxsize=_task_queue_length)
        _task_result_queue = queue.Queue(maxsize=self.parallel)
        _task_error_queue = queue.Queue()
        _task_error_thread_event = threading.Event()

        for _ in range(_task_queue_length):
            t = threading.Thread(
                target=self.worker,
                args=(
                    _task_queue,
                    _task_result_queue,
                    _task_error_thread_event,
                    _task_error_queue,
                ),
            )
            t.start()
            self.logger.debug(f"start worker thread {t.name}")
            _worker_threads.append(t)

        _worker_result_thread = threading.Thread(target=self.result_worker, args=(_task_result_queue,))
        _worker_result_thread.start()
        self.logger.debug(f"start worker result thread {_worker_result_thread.name}")

        _batch_id = 0
        _debug_fetch_rows = 0
        try:
            with recreate_connection(self.source_dsn) as scon:
                for rows in self.get_full_table_orderby_keys(
                    scon,
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
        self.logger.debug(f"queue:[task_queue] {_task_queue_length} - try close.")

        _task_queue.join()
        self.logger.debug(f"queue:[task_queue] - release.")

        for t in _worker_threads:
            t.join()
            self.logger.debug(f"threading:[worker][{t.name}] - release.")

        _task_result_queue.put(None)
        self.logger.debug(f"queue:[task_result_queue] - try close.")
        _task_result_queue.join()
        self.logger.debug(f"queue:[task_result_queue] - release.")

        _worker_result_thread.join()
        self.logger.debug(f"threading:[worker_result][{_worker_result_thread.name}] - release.")

        if not _task_error_queue.empty():
            err = _task_error_queue.get()
            self.logger.error(f"compare failed, {str(err)}.")
            return

        self.logger.info(f"compare completed, processed rows: {self.processed_rows_number} elapsed time: {get_elapsed_time(self.start_timestamp, 2)}s.")

        open(self.done_file, "w").close()

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)


if __name__ == "__main__":
    MysqlTableCompare(
        {"host": "192.168.161.2", "port": 3306, "user": "dtle_sync", "password": "dtle_sync"},
        {"host": "192.168.161.93", "port": 3306, "user": "dtle_sync", "password": "dtle_sync"},
        "merchant_center_vela_v1",
        "mc_products_to_tags",
        "merchant_center_vela_v1",
        "mc_products_to_tags",
    ).run()
