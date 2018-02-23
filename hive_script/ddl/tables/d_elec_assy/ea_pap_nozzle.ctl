-------------------- HEADER ----------------------
-- Name                : ea_pap_nozzle
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2017-09-06
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}elec_assy.ea_pap_nozzle
(
 xml_tag_name                 STRING
,xml_status                   STRING
,bicoe_load_dttm              TIMESTAMP
,xml_tag_id                   STRING
,product_id                   STRING
,date_time                    TIMESTAMP
,file_name                    STRING
,hdfs_file_name               STRING
,kafka_topic                  STRING
,user_name                    STRING
,location                     STRING
,tooltype                     INT 
,ul_picks                     INT
,ul_placements                INT
,ul_comp_missing              INT
,ul_comp_up_side_down         INT
,ul_lead_location_failure     INT
,ul_lead_spacing_failure      INT
,ul_lead_deviation_failure    INT
,ul_lead_span_failure         INT
,ul_comp_location_failure     INT
,ul_compsize_failure          INT
,ul_feature_location_failure  INT
,ul_feature_spacing_failure   INT
,ul_pitch_failure             INT
,ul_orientation_check_failure INT
,ul_lead_count_failure        INT
,ul_ball_count_failure        INT
,ul_feature_count_failure     INT
,ul_possible_missing          INT
,ul_rejects                   INT
,ul_purged                    INT
,ul_corner_tolerance_failure  INT
,ul_vps_height_failure        INT
,ul_vps_part_presence_failure INT
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/iot/raw/${env}elec_assy.db/ea_pap_nozzle';
