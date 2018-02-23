-------------------- HEADER ----------------------
-- Name                : ea_pap_gen
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS d_elec_assy.ea_pap_gen
(
 xml_tag_name                            STRING
,xml_status                              STRING
,bicoe_load_dttm                         TIMESTAMP
,xml_tag_id                              STRING
,product_id                              STRING                
,date_time                               TIMESTAMP
,file_name                               STRING
,hdfs_file_name                          STRING
,kafka_topic                             STRING
,user_name                               STRING
,ul_fid_find_attempts                    INT
,ul_fid_find_failures                    INT
,ul_boards_entering                      INT
,ul_boards_exiting                       INT
,ul_tot_bad_board_sense_accepted         INT
,ul_tot_bad_board_sense_rejected         INT
,ul_tot_bad_circuit_sense_accepted       INT
,ul_tot_bad_circuit_sense_rejected       INT
,ul_tot_board_cycle_time                 INT
,ul_best_board_cycle_time                INT
,ul_board_cycle_count                    INT
,ul_possible_missing                     INT
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_pap_gen';
