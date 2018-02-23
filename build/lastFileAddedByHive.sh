 hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db | awk -F" " '{print $6" "$7" "$8}' | sort -nr | head -1
