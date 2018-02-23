#!/bin/bash

##################################################################################################################################################
## Name: createSchema.sh
## Purpose: Iterate specified list of table for specified database given as arguments and call createTableSchema.sh for every table in the list.
## Author: ksuthar
## Arguments: #1. Database name (string) #2. List of tables (file)
## Created Date: 2017-09-21
## Modified Date: 
## Modification Description:
###################################################################################################################################################

echo "Start Time of $0 "`date`
EXT_STS=0
HLT=1

## Function to print help.
## Name: showHelp
## Purpose: to show usage of the script and format to execute
## Parameters: None
## return Value: Exit code
showHelp () {
   SCRIPT=`basename "$0"`
   echo " Usage: "
   echo -e "\t ${SCRIPT} executes create table script for specified tables(as -t|--tableList) in a specified DB(as -d|--database)."
   echo -e "\t Format to run ${SCRIPT} is: "
   echo -e "\t ./${SCRIPT} -d <DB_NAME> -l <TABLE_LIST> [--skipFailure]"
   echo -e "\t Another way is:"
   echo -e "\t ./${SCRIPT} --database <DB_NAME> --tableList <TABLE_LIST> [--skipFailure]"
   echo -e "\t For usage: "
   echo -e "\t ./${SCRIPT} --help"
   echo ""
   echo " Available options are: "
   echo -e "\t -d|--database : Name of database for which tables need to be created."
   echo -e "\t -l|--tableList : List of tables need to be created for specified database as -d|--database."
   echo -e "\t --skipFailure : To skip failure and continue processing tables from the list, for e.g. If any raw table creation failed --skipFailure aviod exiting from this scripti and continue with next table in the list. Without this option script will exit from point of failure of table creation."
   echo -e "\t -h|--help : show help for ${SCRIPT}."
}

## Checking number of argument passed to script.
if [ ! $# -gt 0 ]
then
   EXT_STS=1000
   echo "[Error_${EXT_STS} in $0]: No arguments provided."
   showHelp
   echo "============================================================================="
   exit ${EXT_STS}
else
   ## iterating all the argumnets and setting variables
   while [[ $# -ge 1 ]]
   do
   option="$1"
   case $option in
       -d|--database)
       DB_NAME=$2
       DB_NAME=`echo "$DB_NAME" | tr '[:upper:]' '[:lower:]'`
       shift ## shift argument sequence
       ;;
       -l|--tableList)
       TABLE_LIST=$2
       shift ## shift argument sequence
       ;;
       --skipFailure)
       HLT=0
       shift ## shift argument sequence
       ;;
       -h|--help)
       showHelp
       exit 0;
       ;;
       *)
       EXT_STS=1001
       echo "[Error_${EXT_STS} in $0]: Invalid arguments."
       showHelp
       echo "============================================================================="
       exit ${EXT_STS}; 
       ;;
   esac
   shift ## shift argument sequence or value
   done
fi
   
## Checking arguments
if [ ! ${DB_NAME} ] || [ ! ${TABLE_LIST} ]
then
   EXT_STS=1002
   echo "[Error_${EXT_STS} in $0]: Database and table list must be privided for creating table."
   echo "try --help option"
   exit ${EXT_STS}
else  
   SCRIPT_LOC="${TWB_BASE}/ddl/tables/${HIVE_ENV}${DB_NAME}"

   ## Checking script location for received DB.
   if [ ! -d ${SCRIPT_LOC} ] ; then
      EXT_STS=1010
      echo "[Error_${EXT_STS} in $0]: Directory ${SCRIPT_LOC} not found !"
      echo -n "$0 finished with error at "
      date
      echo "============================================================================="
      exit ${EXT_STS}
   fi

   ## Checking list of tables, received as argument.
   if [ ! -f ${TABLE_LIST} ] ; then
      EXT_STS=1020
      echo "[Error_${EXT_STS} in $0]: File ${TABLE_LIST} not found !"
      echo -n "$0 finished with error at "
      date
      echo "============================================================================="
      exit ${EXT_STS}
   fi
fi

# export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"
## Executing create script for each table in the specified list of tables as argument.
for table in `cat ${TABLE_LIST}`
do
    ./createTableSchema.sh ${DB_NAME} ${table}
    RC=$?
    if [ $RC -ne 0 ]
    then
       EXT_STS=${RC}
       echo "[Error_${RC} in $0]: ./createTable.sh failed for ${HIVE_ENV}${DB_NAME}.${table} with Return_Code: $RC !"
       if [ ${HLT} -eq 1 ]; then
          echo "--skipFailure is disabled, Exiting."
          echo "------------------------------------------------------------------------------"
          exit ${EXT_STS}
       fi
       echo "--skipFailure is enabled, Continue...."
       echo "------------------------------------------------------------------------------"
    else
       echo "createTable.sh executed successfully for ${HIVE_ENV}${DB_NAME}.${table}"
       echo "------------------------------------------------------------------------------"
    fi
done

echo "End Time for $0 "`date` 
echo "============================================================================="
exit ${EXT_STS}
