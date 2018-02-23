-------------------- HEADER ----------------------
-- Name                : ea_units
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_units
(
 xml_tag_name           STRING
,xml_status             STRING
,bicoe_load_dttm        TIMESTAMP
,xml_tag_id             STRING
,serial_no		STRING
,panel_no  	        STRING
,file_name              STRING
,hdfs_file_name         STRING
,kafka_topic            STRING
,module_no 		STRING
,order_no 		STRING
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_units';
