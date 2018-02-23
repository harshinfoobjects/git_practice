-------------------- HEADER ----------------------
-- Name                : ea_workorder
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 
-------------------- HEADER ----------------------
CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_workorder
(
 xml_tag_name      STRING
,xml_status        STRING
,bicoe_load_dttm   TIMESTAMP
,xml_tag_id        STRING
,file_name         STRING
,hdfs_file_name    STRING
,kafka_topic       STRING
,order_number      STRING
,part_number       STRING
,part_revision     STRING
,description       STRING
,plant_code        STRING
,order_type        STRING
,prod_scheduler    STRING
,release_date      TIMESTAMP
,bom_date          TIMESTAMP
,planned_route     STRING
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_workorder';
