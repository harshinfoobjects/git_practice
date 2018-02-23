-------------------- HEADER ----------------------
-- Name                : ea_ftdefects
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_ftdefects
(
 xml_tag_name                   STRING
,xml_status                     STRING
,bicoe_load_dttm                TIMESTAMP
,xml_tag_id                     STRING
,defect_date                    TIMESTAMP
,placing_plant_code             STRING
,file_name                      STRING
,hdfs_file_name                 STRING
,kafka_topic                    STRING
,placing_area_code              STRING
,placing_process_code           STRING
,placing_wc_code                STRING
,finding_plant_code             STRING
,finding_area_code              STRING
,finding_process_code           STRING
,finding_wc_code                STRING
,build_part_no                  STRING
,build_part_desc                STRING
,build_serial_no                STRING
,assem_part_no                  STRING
,assem_part_desc                STRING
,assem_serial_no                STRING
,order_no                       STRING
,defect_code                    STRING
,failed_test_code               STRING
,fail_type                      STRING
,fault_board_no                 INT
,cmpnt_part_no                  STRING
,cmpnt_part_desc                STRING
,ref_designator                 STRING
,defect_count                   DECIMAL(10,5)
,repair_date                    TIMESTAMP
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_ftdefects';
