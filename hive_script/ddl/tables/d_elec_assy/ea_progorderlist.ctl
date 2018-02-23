-------------------- HEADER ----------------------
-- Name                : ea_progorderlist
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_progorderlist
(
 xml_tag_name    STRING
,xml_status      STRING
,bicoe_load_dttm TIMESTAMP
,xml_tag_id      STRING                
,plant           STRING    
,machine         STRING
,file_name       STRING
,hdfs_file_name  STRING
,kafka_topic     STRING
,work_order      STRING
,assembly        STRING
,revision        STRING
,side            STRING
,part_number     STRING
,slot            STRING
,head            STRING
,spindle         STRING
,ref_id          STRING
,time_stamp      TIMESTAMP
,circuit         STRING
,silkscreen      BIGINT
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_progorderlist';
