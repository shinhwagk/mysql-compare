#!/bin/bash

find ./ -type f -name '*.err.log' -printf '%P\n' | sed 's/.err.log//' | while read line; do
    if [[ `cat ${line}.err.log` == "not have primary key or unique keys." ]]; then
        mysql -ughost -p54448hotINBOX -h10.50.10.165 -P3307 -e "set session time_zone='+00:00'; select * from ${line}" >"${line}.src.data"
        mysql -ughost -p54448hotINBOX -h10.50.0.198 -P3317 -e "set session time_zone='+00:00'; select * from ${line}" >"${line}.dst.data"
        SRC_SS=`sha1sum "${line}.src.data" | awk '{print $1}'`
        DST_SS=`sha1sum "${line}.dst.data" | awk '{print $1}'`
        [[ $SRC_SS == $DST_SS ]] || echo "diff: $line"
    fi
done