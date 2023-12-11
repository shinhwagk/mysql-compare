import argparse

from mysql_compare.mysql_compare import MysqlTableCompare

parser = argparse.ArgumentParser(prog="ProgramName", description="What the program does", epilog="Text at the bottom of help")
parser.add_argument("--source-dsn", type=str, required=True)
parser.add_argument("--target-dsn", type=str, required=True)
parser.add_argument("--src-database", type=str, required=True)
parser.add_argument("--src-table", type=str, required=True)
parser.add_argument("--dst-database", type=str, required=True)
parser.add_argument("--dst-table", type=str, required=True)
args = parser.parse_args()

_userpass, _hostport = args.source_dsn.split("@")
_user, _pass = _userpass.split("/")
_host, _port = _hostport.split(":")
_source_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

_userpass, _hostport = args.target_dsn.split("@")
_user, _pass = _userpass.split("/")
_host, _port = _hostport.split(":")
_target_dsn = {"host": _host, "port": _port, "user": _user, "password": _pass}

MysqlTableCompare(_source_dsn, _target_dsn, args.src_database, args.src_table, args.dst_database, args.dst_table).run()
