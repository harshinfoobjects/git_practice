-------------------- HEADER ----------------------
-- Name                : ea_wodetail
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_wodetail
(
 xml_tag_name                    STRING
,xml_status                      STRING
,bicoe_load_dttm                 TIMESTAMP
,xml_tag_id                      STRING
,plant				 STRING
,optel_schedule_wo  	         STRING
,file_name                       STRING
,hdfs_file_name                  STRING
,kafka_topic                     STRING
,sap_wo 			 STRING
,assembly 			 STRING
,assembly_rev 			 STRING
,build 			         STRING
,build_rev 			 STRING
,order_qty  			 INT			
,panel_qty 			 DECIMAL(10,5)
,side 				 STRING
,setup 			         DECIMAL(10,5)
,golden 			 STRING
,pipplc_panel 			 DECIMAL(10,5)
,pippad_panel  		         DECIMAL(10,5)
,smtplc_panel 			 DECIMAL(10,5)
,smtpad_panel 			 DECIMAL(10,5)
,line 				 STRING
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_wodetail';
