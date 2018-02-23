-------------------- HEADER ----------------------
-- Name                : ea_equipmentoutline
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_equipmentoutline
(
 xml_tag_name                    STRING
,xml_status                      STRING
,bicoe_load_dttm                 TIMESTAMP
,xml_tag_id                      STRING
,plant                           STRING
,equipment_make                  STRING
,file_name                       STRING
,hdfs_file_name                  STRING
,kafka_topic                     STRING
,machine_name                    STRING
,line                            STRING
,head_1                          STRING
,head_2                          STRING
,timestamp                       TIMESTAMP
,equipment_model                 STRING
,equipment_type                  STRING
,position                        DECIMAL(10,5)    

)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_equipmentoutline';
