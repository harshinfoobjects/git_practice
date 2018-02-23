-------------------- HEADER ----------------------
-- Name                : ea_spi_detail
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_spi_detail
(
 xml_tag_name                    STRING
,xml_status                      STRING
,bicoe_load_dttm                 TIMESTAMP
,xml_tag_id                      STRING
,file_name                       STRING
,hdfs_file_name                  STRING
,kafka_topic                     STRING
,generated_key                   STRING
,module_nbr                      INT
,ref_designator                  STRING
,pin_nbr                         STRING
,size_x                          INT
,size_y                          INT
,pad_stencil_height              DECIMAL(5,2)
,pad_result                      STRING
,pad_defect_type                 STRING
,solder_paste_volume             DECIMAL(10,5)
,solder_paste_height             DECIMAL(10,5)
,solder_paste_area               DECIMAL(10,5)
,offset_X                        DECIMAL(10,5)
,offset_Y                        DECIMAL(10,5)
)
  STORED AS PARQUET
    LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_spi_detail';
