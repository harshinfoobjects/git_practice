-------------------- HEADER ----------------------
-- Name                : ea_spi_header
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_spi_header
(
 xml_tag_name                    STRING
,xml_status                      STRING
,bicoe_load_dttm                 TIMESTAMP
,xml_tag_id                      STRING
,file_name                       STRING
,hdfs_file_name                  STRING
,kafka_topic                     STRING
,barcode                         STRING
,side                            STRING
,pcb_id                          STRING
,inspection_start_dttm           TIMESTAMP
,inspection_end_dttm             TIMESTAMP
,inspection_test_duration_sec    DECIMAL(5,2)
,pcb_name                        STRING
,pcb_result                      STRING
,print_stroke                    STRING
,machine_id                      STRING
,generated_key                   STRING
)
  STORED AS PARQUET
    LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_spi_header';
