echo 'db1.tab1
db2.tab2' | while read line; do
echo $line
[[ -f "${line}.src.data" ]] || mysql -ughost -p54448hotINBOX -h10.50.0.234 -P3307 -e "select * from ${line}" >"${line}.src.data"
[[ -f "${line}.dst.data" ]] || mysql -ughost -p54448hotINBOX -h10.50.0.204 -P3306 -e "select * from ${line}" >"${line}.dst.data"
done


echo 'db1.tab1
db2.tab2'  | while read line; do
echo $line
SRC_SS=`sha1sum "${line}.src.data" | awk '{print $1}'`
DST_SS=`sha1sum "${line}.dst.data" | awk '{print $1}'`
[[ $SRC_SS == $DST_SS ]] || echo "diff: $line"
done
