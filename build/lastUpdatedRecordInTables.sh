pipe=" |"
echo "+---------------------------------------+"
echo "| TableName     |  Last Updated Time    |"
echo "+---------------------------------------+"
echo $(printf "%s %s %s" "|  inbound_pap       | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/inbound/d_elec_assy_in/*_pap* | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  inbound_md      | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/inbound/d_elec_assy_in/*_md* | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  inbound_spi       | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/inbound/d_elec_assy_in/*_spi* | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)


echo $(printf "%s %s %s" "|  ea_spi_detail       | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_spi_detail | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_spi_header       | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_spi_header | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_pap_nozzle       | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_pap_nozzle | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_pap_info         | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_pap_info | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_pap_slot         | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_pap_slot | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_pap_gen          | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_pap_gen | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_pap_timers       | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_pap_timers | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_equipmentoutline | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_equipmentoutline | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_comptype         | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_comptype | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_parentchild      | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_parentchild | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_workorder        | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_workorder | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_assycircuitsilk  | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_assycircuitsilk | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_wodetail         | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_wodetail | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_units            | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_units | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_assydetail       | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_assydetail | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_ftdefects        | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_ftdefects | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo $(printf "%s %s %s" "|  ea_progorderlist    | " & hadoop fs -ls -R /user/hive/warehouse/rockwell/iot/raw/d_elec_assy.db/ea_progorderlist | awk -F" " '{print $6" "$7}' | sort -nr | head -1 && echo  $pipe)
echo "+---------------------------------------+"
