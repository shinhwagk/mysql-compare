echo 'merchant_center_vela_v1.tmp_p
merchant_center_vela_v1.tmp_sell
merchant_center_vela_v1.tmp_tariff_payout_rules_vat_guoting
merchant_center_vela_v1.v3_test
test.mc_discount_rule_products2_his_ador_0616' | while read line; do
echo $line
mysql -ughost -p54448hotINBOX -h172.23.8.44 -P3308 -e "select * from ${line}" >"${line}.src.data"
mysql -ughost -p54448hotINBOX -h172.23.8.44 -P3318 -e "select * from ${line}" >"${line}.dst.data"
done


echo 'merchant_center_vela_v1.v3_test
test.mc_discount_rule_products2_his_ador_0616' | while read line; do
echo $line
SRC_SS=`sha1sum "${line}.src.data" | awk '{print $1}'`
DST_SS=`sha1sum "${line}.dst.data" | awk '{print $1}'`
[[ $SRC_SS == $DST_SS ]] || echo "diff: $line"
done
