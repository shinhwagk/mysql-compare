a simple table compare tool
### usage
```sh
pip install mysql-compare
python -m mysql_compare --source-dsn user/pass@host:ip --target-dsn user/pass@host:ip --database db --table table

# logfile
1. {db}.{table}.err.log
2. {db}.{table}.done
3. {db}.{table}.diff.log
```
