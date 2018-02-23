#!/bin/bash

##################################################################################################################################################
## Name: createTableSchema.sh
## Purpose: Create specified tables in a specified DB given as arguments.
## Author: ksuthar
## Arguments: #1. Database name (string) #2. Table name (string)
## Created Date: 2017-09-13
## Modified Date: 
## Modification Description: 
###################################################################################################################################################

echo "Start Time of $0 "`date`

## Checking number of argument passed to script.
if [ $# -ne 2 ]
then
    echo "[Error_1000 in $0]: Invalid arguments, should be 2 <db_name> <table_name>"
    echo "Expected format is $0 <db_name> <table_name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1000
else
  ## Setting variables by arguments
  DB_NAME=$1
  DB_NAME=`echo "$DB_NAME" | tr '[:upper:]' '[:lower:]'`
  SCRIPT_LOC="${TWB_BASE}/ddl/tables/${HIVE_ENV}${DB_NAME}"
  TABLE=$2
  TABLE=`echo "${TABLE}" | tr '[:upper:]' '[:lower:]'`

  ## Checking script location for received as argument. 
  if [ ! -d ${SCRIPT_LOC} ] ; then
    echo "[Error_1010 in $0]: Directory ${SCRIPT_LOC} not found !"
    echo "Expected format is $0 <db_name> <table_name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1010
  fi

  ## Checking script for table received as argument. 
  if [ ! -f ${SCRIPT_LOC}/${TABLE}.ctl ] ; then
    echo "[Error_1020 in $0]: File ${SCRIPT_LOC}/${TABLE}.ctl not found !"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1020
  fi
fi

script="${SCRIPT_LOC}/${TABLE}.ctl"

echo " "
echo "TABLE: ${TABLE}, SCRIPT: ${script}"
echo " "


beeline -u ${HIVE_URL} ${HIVE_USER} ${HIVE_CONF} --hivevar env=${HIVE_ENV} -f ${script}
RC=$?
if [ $RC -ne 0 ]
then
     echo "[Error_1050 in $0]: Beeline command for ${script} failed with Return_Code: $RC !"
     echo "[COMMAND]: beeline -u ${HIVE_URL} ${HIVE_USER} ${HIVE_CONF} --hivevar env=${HIVE_ENV} -f ${script}"
     echo "------------------------------------------------------------------------------"
     exit 1050
fi
echo "${DB_NAME}.${TABLE} Created Successfully"
echo "------------------------------------------------------------------------------"

echo "End Time for $0 "`date`
exit 0
