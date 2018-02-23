-------------------- HEADER ----------------------
-- Name                : oracle_optel_twb_comptype
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_comptype
(
 xml_tag_name                           STRING
,xml_status                             STRING
,bicoe_load_dttm                        TIMESTAMP
,xml_tag_id                             STRING
,plant					STRING
,machine				STRING
,part_number			        STRING
,file_name                              STRING
,hdfs_file_name                         STRING
,kafka_topic                            STRING
,package				STRING
,feedertype				STRING
,head_type				STRING
,nozzle1				STRING
,nozzle2				STRING
,pressure				DECIMAL(10,5)
,accuracy				STRING
,preorient				STRING
,apu					STRING
,pick 					STRING
,motion 				STRING
,center					STRING
,place 					STRING
,lpid					DECIMAL(10,5)
,pins					DECIMAL(10,5)
,max_of_inv				DECIMAL(10,5)
,comp_height			        DECIMAL(10,5)
,comp_length			        DECIMAL(10,5)
,comp_width				DECIMAL(10,5)
,body_length            		DECIMAL(10,5)
,body_width				DECIMAL(10,5)
,type					STRING
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_comptype';

