-------------------- HEADER ----------------------
-- Name                : ea_parentchild
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_parentchild
(
 xml_tag_name            STRING
,xml_status              STRING
,bicoe_load_dttm         TIMESTAMP
,xml_tag_id              STRING
,parent_serial_no        STRING
,child_serial_number     STRING
,file_name               STRING
,hdfs_file_name          STRING
,kafka_topic             STRING
,child_finish_date       TIMESTAMP
,consumption_date        TIMESTAMP
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_parentchild';


