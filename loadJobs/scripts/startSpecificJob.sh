#!/bin/bash

##################################################################################################################################################
## Name: startSpecificJob.sh
## Purpose: Start/Restart specified spark job in a specified Project given as arguments.
## Author: ksuthar
## Arguments: #1. Project name (string) #2. Job name (string)
## Created Date: 2017-12-12
## Modified Date: 
## Modification Description: 
###################################################################################################################################################

echo "Start Time of $0 "`date`

## Checking number of argument passed to script.
if [ $# -ne 3 ]
then
    echo "[Error_1000 in $0]: Invalid arguments, should be 3 <project_name> <job_name> <restart as yes/no>"
    echo "Expected format is $0 <project_name> <job_name> <restart as yes/no>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1000
else
  ## Setting variables by arguments
  PROJECT_NAME=$1
  PROJECT_NAME=`echo "$PROJECT_NAME" | tr '[:upper:]' '[:lower:]'`
  SCRIPT_LOC="${TWB_BASE}/config/jobs/${PROJECT_NAME}"
  JOB=$2
  JOB=`echo "${JOB}" | tr '[:upper:]' '[:lower:]'`
  RESTART=$3
  RESTART=`echo "${RESTART}" | tr '[:upper:]' '[:lower:]'`

  ## Checking restart flag
  if ! [[ "$RESTART" == "yes" || "$RESTART" == "no" ]]; then
    echo "[Error_1000 in $0]: Invalid arguments, -r|--restart must be yes|no"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1010
  fi 

  ## Checking script location for received as argument. 
  if [ ! -d ${SCRIPT_LOC} ] ; then
    echo "[Error_1010 in $0]: Directory ${SCRIPT_LOC} not found !"
    echo "Expected format is $0 <project_name> <job_name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1010
  fi

  ## Checking script for JOB received as argument. 
  if [ ! -f ${SCRIPT_LOC}/${JOB}.sh ] ; then
    echo "[Error_1020 in $0]: File ${SCRIPT_LOC}/${JOB}.sh not found !"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1020
  fi
fi

if [ $RESTART == "yes" ]; then
  ./killJob.sh ${JOB}
  RC=$?
  if [ $RC -ne 0 ]
  then
     EXT_STS=${RC}
     echo "[Error_${RC} in $0]: ./killJob.sh failed for ${JOB} with Return_Code: $RC !"
     if [ ${HLT} -eq 1 ]; then
        echo "--skipFailure is disabled, Exiting."
        echo "------------------------------------------------------------------------------"
        exit ${EXT_STS}
     fi
     echo "--skipFailure is enabled, Continue...."
     echo "------------------------------------------------------------------------------"
  else
     echo "killJob.sh executed successfully for ${JOB}"
     echo "------------------------------------------------------------------------------"
  fi
fi

script="${SCRIPT_LOC}/${JOB}.sh"

echo " "
echo "JOB: ${JOB}, SCRIPT: ${script}"
echo " "

sh "${SCRIPT_LOC}/${JOB}".sh
RC=$?
if [ $RC -ne 0 ]
then
   EXT_STS=${RC}
   echo "[Error_${RC} in $0]: ./${JOB}.sh failed with Return_Code: $RC !"
   if [ ${HLT} -eq 1 ]; then
      echo "--skipFailure is disabled, Exiting."
      echo "------------------------------------------------------------------------------"
      exit ${EXT_STS}
   fi
   echo "--skipFailure is enabled, Continue...."
   echo "------------------------------------------------------------------------------"
else
   echo "${JOB}.sh executed successfully for ${JOB}"
   echo "------------------------------------------------------------------------------"
fi

echo "Project : ${PROJECT_NAME} JOB: ${JOB} started successfully"
echo "Check log at location : ${TWB_BASE}/loadJobs/logs/${JOB}.log"
echo "------------------------------------------------------------------------------"

echo "End Time for $0 "`date`
exit 0
