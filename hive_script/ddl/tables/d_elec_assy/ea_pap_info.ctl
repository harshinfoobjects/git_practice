-------------------- HEADER ----------------------
-- Name                : ea_pap_info
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_pap_info
(
 xml_tag_name    STRING
,xml_status      STRING
,bicoe_load_dttm TIMESTAMP
,xml_tag_id      STRING
,name            STRING
,is_keyed        Boolean
,file_name       STRING
,hdfs_file_name  STRING
,kafka_topic     STRING
,crc_id          INT
,description     STRING
,shape           INT
,notes           STRING
,plant_code      STRING
,machine         STRING
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_pap_info';
