#!/bin/bash

# file: env_vars
# export ARGS_SOURCE_DSN=ghost/54448hotINBOX@172.16.0.179:3306
# export ARGS_TARGET_DSN=dtle_sync/Ej4VFMyQ7wjRjtfX@172.16.0.67:3306
# export ARGS_LOG_LOCATION=/opt

# docker rm -f mysql-compare-litbmc
# docker run -d --name mysql-compare-litbmc -w /opt/ \
# -e TZ=Asia/Shanghai
# -v /opt/mysql-compare/litbmc:/opt \
# -v /opt/mysql-compare/lib:/app \
# python:3 bash /app/startup.sh
# docker logs -f mysql-compare-litbmc

set -e

. env_vars

env | grep ARGS

pip install mysql-compare -U

PY_SCRIPT_COMPARE=/app/dbcompare.py
PY_SCRIPT_NOKEY=/app/dbnokey.py
PY_SCRIPT_REPAIR=/app/dbrepair.py

while true; do
    today=$(date +%Y%m%d)
    latest_dir=$(find . -maxdepth 1 -mindepth 1 -type d -name '[0-9]*' -printf '%P\n' | sort -r | head -n 1)

    if [[ -z "${latest_dir}" || -f "${latest_dir}.done" ]]; then
        dirname="$today"
    else
        dirname="$latest_dir"
    fi

    mkdir -p "$dirname"
    donefile="${dirname}.done"

    if [[ ! -f "$donefile" ]]; then
        echo "Running task in directory: $dirname"
        echo "Start task for dbcompare"
        (cd "$dirname" && python -u $PY_SCRIPT_COMPARE)
        echo "Start task for dbnokey"
        if [[ ! -f "${dirname}/dbnokey.done" ]]; then
            (cd "$dirname" && python -u $PY_SCRIPT_NOKEY >dbnokey.log && touch dbnokey.done)
        fi
        echo "Start task for dbrepair"
        if [[ ! -f "${dirname}/dbrepair.done" ]]; then
            (cd "$dirname" && python -u $PY_SCRIPT_REPAIR >dbrepair.log && touch dbrepair.done)
        fi
        touch "$donefile"
        echo "Task completed for $dirname."
    else
        echo "Task for today ($dirname) is already completed."
        sleep 60
    fi
done
