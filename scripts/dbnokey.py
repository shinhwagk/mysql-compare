import hashlib
import os

from mysql.connector import MySQLConnection, connect


def sha256sum(filepath: str) -> str:
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(4096):
            sha256.update(chunk)
    return sha256.hexdigest()


def write_table_data_to_file(con: MySQLConnection, database, table, datafile):
    with con.cursor(dictionary=True) as cursor, open(datafile, "w", encoding="utf8") as file:
        cursor.execute(f"SELECT * FROM {database}.{table}")
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                file.write(f"{row}\n")


def write_rows(datafile: str, rows: list[dict]):
    with open(datafile, "a", encoding="utf8") as f:
        f.writelines(f"{row}\n" for row in rows)


def create_connection(dsn):
    user_pass, host_port = dsn.split("@")
    user, password = user_pass.split("/")
    host, port = host_port.split(":")
    return connect(host=host, port=port, user=user, password=password, time_zone="+00:00")


def compare_tables(source_dsn: str, target_dsn: str, database, table):
    src_file = f"{database}.{table}.src.nokey.log"
    dst_file = f"{database}.{table}.dst.nokey.log"

    with create_connection(source_dsn) as source_con:
        write_table_data_to_file(source_con, database, table, src_file)

    with create_connection(target_dsn) as target_con:
        write_table_data_to_file(target_con, database, table, dst_file)

    if sha256sum(src_file) != sha256sum(dst_file):
        print(f"nokey table compare: {database}.{table} different.")
    else:
        print(f"nokey table compare: {database}.{table} same.")

    source_con.close()
    target_con.close()


if __name__ == "__main__":
    ARGS_SOURCE_DSN = os.environ.get("ARGS_SOURCE_DSN")
    ARGS_TARGET_DSN = os.environ.get("ARGS_TARGET_DSN")

    _log_location = os.getcwd()
    error_files = [f for f in os.listdir(_log_location) if f.endswith(".err.log")]

    for f in error_files:
        with open(os.path.join(_log_location, f), "r", encoding="utf8") as errfile:
            if "not have primary key or unique keys" in errfile.readline():
                _database, _table = f.split(".")[:2]
                print(f"nokey table compare start {_database}.{_table}")
                compare_tables(ARGS_SOURCE_DSN, ARGS_TARGET_DSN, _database, _table)

    print("nokey table compare complete.")
