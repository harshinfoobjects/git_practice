 hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/inbound/d_elec_assy_in | awk -F" " '{print $6" "$7" "$8}' | sort -nr | head -1
