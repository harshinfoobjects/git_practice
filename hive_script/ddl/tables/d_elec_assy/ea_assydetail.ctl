

-------------------- HEADER ----------------------
-- Name                : ea_assydetail
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_assydetail
(
 xml_tag_name           STRING
,xml_status             STRING
,bicoe_load_dttm        TIMESTAMP
,xml_tag_id             STRING
,plant                  STRING
,assembly               STRING
,rev                    STRING
,file_name              STRING
,hdfs_file_name         STRING
,kafka_topic            STRING
,side                   STRING
,pcb                    STRING
,family                 DECIMAL(10,5)
,family_description     STRING
,side_glue              STRING
,circuits_quantity      DECIMAL(10,5)
,pcb_length             DECIMAL(10,5)
,pcb_width              DECIMAL(10,5)
,placement_panel        DECIMAL(10,5)
,flat_program           STRING
,stencil_name           STRING
,thickness              DECIMAL(10,5)
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_assydetail';
