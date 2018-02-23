-------------------- HEADER ----------------------
-- Name                : ea_pap_timers
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_pap_timers
(
 xml_tag_name                        STRING
,xml_status                          STRING
,bicoe_load_dttm                     TIMESTAMP
,xml_tag_id                          STRING
,product_id                          STRING
,date_time                           TIMESTAMP
,file_name                           STRING
,hdfs_file_name                      STRING
,kafka_topic                         STRING
,user_name                           STRING
,ul_production_time                  INT
,ul_setup_time                       INT
,ul_idle_time                        INT
,ul_diagnostic_time                  INT
,ul_waiting_for_operator             INT
,ul_waiting_for_board_input          INT
,ul_waiting_for_board_output         INT
,ul_waiting_for_interrupt_recovery   INT
,ul_waiting_for_board_in_workarea    INT
,ul_waiting_for_brd_from_upline      INT
,ul_waiting_for_downline_available   INT
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_pap_timers';
