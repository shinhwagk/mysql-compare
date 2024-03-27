import concurrent.futures
import datetime
import itertools
import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass
from decimal import Decimal

from mysql.connector import MySQLConnection, connect
from mysql.connector.pooling import MySQLConnectionPool


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
        SELECT tis.index_name, titc.constraint_type, tic.column_name, tic.data_type
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


def get_elapsed_time(st: float, ndigits=None) -> int | float:
    return round(time.time() - st, ndigits)


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

        self.limit_size = fetch_size
        self.fetch_size = shard_size

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

        if self.limit_size % self.fetch_size != 0:
            raise Exception(f"The remainder of the fetch_size divided by the shard_size must be equal to 0.")

        self.logger = init_logger(self.compare_name)

        self.logger.info(f"source table connect pool create.")
        self.source_conpool = MySQLConnectionPool(pool_name="source_conpool", pool_size=math.ceil(self.parallel * 1.2) + 4, **self.source_dsn)

        self.logger.info(f"target table connect pool create.")
        self.target_conpool = MySQLConnectionPool(pool_name="target_conpool", pool_size=math.ceil(self.parallel * 1.2), **self.target_dsn)

    # batch_id, source_key_vals, diff_rows
    def processing_result(self, cur_batch_id, _tasks: list[tuple[int, list[dict], list[dict]]]):
        _batch_id = cur_batch_id

        _tasks.sort(key=lambda x: x[0])

        while _tasks and _tasks[0][0] == _batch_id:
            batch_id, source_key_vals, diff_rows = _tasks.pop(0)

            if diff_rows:
                self.write_different_rows(diff_rows)
                self.different_rows_number += len(diff_rows)

            self.processed_rows_number += len(source_key_vals)

            if self.processed_rows_number % 10000 == 0:
                self.write_checkpoint(source_key_vals[-1], self.processed_rows_number, self.different_rows_number)
                self.logger.info(f"checkpoint: {source_key_vals[-1]}")

            _progress_rate = round(self.processed_rows_number / max(1, self.source_table_rows_number) * 100, 1)
            self.logger.debug(
                f"batch[{batch_id}] - compare progress - {_progress_rate}%, different: {self.different_rows_number}, total rows: {self.source_table_rows_number}."
            )

            _batch_id += 1

        return _batch_id

    def write_different_rows(self, rows: list[dict]):
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

    def get_table_rows_by_keys(self, con: MySQLConnectionPool, database, table, table_keys_rows: list[dict]) -> list[dict]:
        cols = [key[0] for key in self.source_table_keys]
        placeholders = ", ".join(["%s"] * len(cols))

        in_clause = ", ".join([f"({placeholders})" for _ in table_keys_rows])

        params = [val for row in table_keys_rows for val in (row[col] for col in cols)]

        formatted_cols = [f"`{c}`" for c in cols]
        _stmt = f"SELECT * FROM {database}.{table} WHERE ({', '.join(formatted_cols)}) IN ({in_clause})"

        with con.get_connection() as con:
            with con.cursor(dictionary=True) as cur:
                cur.execute(_stmt, tuple(params))
                return cur.fetchall()

    def get_full_table_keys_order(self, source_con: MySQLConnection, keycols: list[tuple[str, str]], ckpt_row: dict = None):
        _keyval = ckpt_row
        # select * from where 1 = 1 and ((a > xxx) or (a = xxx and b > yyy) or (a = xxx and b = yyy and c > zzz)) order by a,b,c limit checksize
        params: list = []
        _key_colns = ", ".join([f"`{col[0]}`" for col in keycols])

        while True:
            if _keyval is None:
                statement = f"SELECT {_key_colns} FROM {self.src_database}.{self.src_table} ORDER BY {_key_colns} limit {self.limit_size}"
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
                statement = f"SELECT {_key_colns} FROM {self.src_database}.{self.src_table} WHERE {where} ORDER BY {_key_colns} limit {self.limit_size}"

            self.logger.debug(f"threading:[{threading.current_thread().name}] - compare ready - sql: {statement}, params: {params}")

            _has_data = False

            with source_con.cursor(dictionary=True) as cur:
                if len(params) == 0:
                    cur.execute(statement)
                else:
                    cur.execute(statement, params=tuple(params.copy()))
                params.clear()

                while True:
                    rows = cur.fetchmany(size=self.fetch_size)
                    if rows:
                        _keyval = rows[-1]
                        _has_data = True
                        yield rows
                    else:
                        break

            if not _has_data:
                return

    def compare_rows_by_keys(self, batch_id: int, source_key_vals: list[dict]):
        self.logger.debug(f"batch[{batch_id}] - compare start - rows {len(source_key_vals)}.")

        tasks = {
            "source": (self.source_conpool, self.src_database, self.src_table, source_key_vals),
            "target": (self.target_conpool, self.dst_database, self.dst_table, source_key_vals),
        }

        fetch_rows = lambda db_conpool, database, table, key_vals: self.get_table_rows_by_keys(db_conpool, database, table, key_vals)

        _s_ts = time.time()

        def execute_tasks(retry_count=5):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(fetch_rows, *tasks[task]): task for task in tasks}
                try:
                    return {task: future.result() for future, task in futures.items()}
                except Exception as e:
                    self.logger.error(f"batch[{batch_id}] - compare success - try: {5 - retry_count}/5, error: {str(e)}  .")
                    if retry_count > 1:
                        return execute_tasks(retry_count - 1)
                    else:
                        raise e

        tasks_result = execute_tasks()

        diff_rows = list(itertools.filterfalse(lambda x: x in tasks_result["target"], tasks_result["source"]))

        self.logger.debug(
            f"batch[{batch_id}] - compare success - elapsed time {get_elapsed_time(_s_ts,2)}s, rows: {len(tasks_result['source'])}, different: {len(diff_rows)}."
        )

        return diff_rows

    def compare_full_table(self, _checkpoint_row: dict):
        _cur_batch_id = 1
        # batch_id, source_key_vals, diff_rows = _tasks.pop(0)
        _result_container: list[tuple[int, list[dict], list[dict]]] = []

        with self.source_conpool.get_connection() as source_con:
            fulltasks = enumerate(self.get_full_table_keys_order(source_con, self.source_table_keys, _checkpoint_row), start=1)

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel) as executor:
                generator_futures = lambda n: {executor.submit(self.compare_rows_by_keys, *task): task for task in itertools.islice(fulltasks, n)}

                futures = generator_futures(self.parallel)

                while futures:
                    done, _ = concurrent.futures.wait(futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED)

                    for fut in done:
                        _result_container.append([*futures.pop(fut), fut.result()])

                    futures.update(generator_futures(len(done)))

                    _cur_batch_id = self.processing_result(_cur_batch_id, _result_container)

    def run(self) -> None:
        try:
            self._run()
        except Exception as e:
            self.logger.error(str(e))

    def _run(self) -> None:
        if os.path.exists(self.done_file):
            return

        with self.source_conpool.get_connection() as source_con, self.target_conpool.get_connection() as target_con:
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

        _checkpoint = self.read_checkpoint()
        _checkpoint_row = None

        if _checkpoint:
            self.logger.info(f"checkpoint: {_checkpoint}")
            self.processed_rows_number = _checkpoint.processed
            self.different_rows_number = _checkpoint.different
            _checkpoint_row = _checkpoint.checkpoint

        self.logger.info(f"from checkpoint: {_checkpoint}")

        self.compare_full_table(_checkpoint_row)  # main

        self.logger.info(
            f"compare completed, processed rows: {self.processed_rows_number}, different: {self.different_rows_number},  elapsed time: {get_elapsed_time(self.start_timestamp, 2)}s."
        )

        open(self.done_file, "w").close()

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)


if __name__ == "__main__":
    MysqlTableCompare(
        {"host": "192.168.161.2", "port": 3306, "user": "dtle_sync", "password": "dtle_sync", "time_zone": "+00:00"},
        {"host": "192.168.161.93", "port": 3306, "user": "dtle_sync", "password": "dtle_sync", "time_zone": "+00:00"},
        "merchant_center_vela_v1",
        "mc_products_to_tags",
        "merchant_center_vela_v1",
        "mc_products_to_tags",
        10,
    ).run()
