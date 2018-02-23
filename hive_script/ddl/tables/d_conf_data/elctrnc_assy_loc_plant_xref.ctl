-------------------- HEADER ----------------------
-- Name                : elctrnc_assy_loc_plant_xref
-- Purpose             : To create hive external table.
-- Author              : ksuthar
-- Date Created        : 2018-02-20
-- Date Modified       : 
-- Modified By         : 
-- Modification Desc   : 

-------------------- HEADER ----------------------

CREATE EXTERNAL TABLE IF NOT EXISTS ${env}conf_data.elctrnc_assy_loc_plant_xref
(
 location    STRING
,plant       STRING
)
  STORED AS PARQUET
   LOCATION '/user/hive/warehouse/rockwell/enterprise/master/${env}conf_data.db/elctrnc_assy_loc_plant_xref';

