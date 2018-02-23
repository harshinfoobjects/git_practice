-------------------- HEADER ----------------------
-- Name                : ea_assycircuitsilk
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_assycircuitsilk
(
 xml_tag_name                           STRING
,xml_status                             STRING
,bicoe_load_dttm                        TIMESTAMP
,xml_tag_id                             STRING
,file_name                              STRING
,hdfs_file_name                         STRING
,kafka_topic                            STRING
,plant  				STRING	
,assembly				STRING
,revision				STRING
,side					STRING
,circuit		                INT
,silkscreen				INT
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_assycircuitsilk';

