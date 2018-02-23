#!/bin/bash

##################################################################################################################################################
## Name: startJobs.sh
## Purpose: start c1 and c4 spark streaming jobs. If job if already started, it restart the job
## Author: ksuthar
## Arguments: #1. job name (string)
## Created Date: 2017-12-12
## Modified Date: 
## Modification 
## Modified Date: 
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
   echo -e "\t ${SCRIPT} start job (as -j|--jobList) in a specified PROJECT(as -p|--project) with restart flag (as -r|--restart).  "
   echo -e "\t Format to run ${SCRIPT} is: "
   echo -e "\t ./${SCRIPT} -p <PROJECT_NAME> -j <JOB_LIST> -r <RESTART_FLAG>"
   echo -e "\t Another way is:"
   echo -e "\t ./${SCRIPT} --project <PROJECT_NAME> --jobList <JOB_LIST> --restart <RESTART_FLAG>"
   echo -e "\t For usage: "
   echo -e "\t ./${SCRIPT} --help"
   echo ""
   echo " Available options are: "
   echo -e "\t -p|--project : Name of database for which jobs need to be started."
   echo -e "\t -j|--jobList : List of jobs need to be started for specified project as -p|--project."
   echo -e "\t -r|--restart : Argument (yes/no) to restart jobs for specified project as -p|--project."
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
       -p|--project)
       PROJECT_NAME=$2
       PROJECT_NAME=`echo "$PROJECT_NAME" | tr '[:upper:]' '[:lower:]'`
       shift ## shift argument sequence
       ;;
       -j|--jobList)
       JOB_LIST=$2
       shift ## shift argument sequence
       ;;
       -r|--restart)
       RESTART=$2
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
if [ ! ${PROJECT_NAME} ] || [ ! ${JOB_LIST} ] || [ ! ${RESTART} ]
then
   EXT_STS=1002
   echo "[Error_${EXT_STS} in $0]: Project, job list and restart flag must be privided for starting job."
   echo "try --help option"
   exit ${EXT_STS}
else  
   SCRIPT_LOC="${TWB_BASE}/config/jobs/${PROJECT_NAME}"

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
   if [ ! -f ${JOB_LIST} ] ; then
      EXT_STS=1020
      echo "[Error_${EXT_STS} in $0]: File ${JOB_LIST} not found !"
      echo -n "$0 finished with error at "
      date
      echo "============================================================================="
      exit ${EXT_STS}
   fi
fi

RESTART=`echo "$RESTART" | tr '[:upper:]' '[:lower:]'`
for JOB in `cat ${JOB_LIST}`
do
    ./startSpecificJob.sh ${PROJECT_NAME} ${JOB} ${RESTART}
    RC=$?
    if [ $RC -ne 0 ]
    then
       EXT_STS=${RC}
       echo "[Error_${RC} in $0]: ./startSpecificJob.sh failed for ${PROJECT_NAME} ${JOB} ${RESTART} with Return_Code: $RC !"
       if [ ${HLT} -eq 1 ]; then
          echo "--skipFailure is disabled, Exiting."
          echo "------------------------------------------------------------------------------"
          exit ${EXT_STS}
       fi
       echo "--skipFailure is enabled, Continue...."
       echo "------------------------------------------------------------------------------"
    else
       echo "startSpecificJob.sh executed successfully for ${PROJECT_NAME} ${JOB} ${RESTART}"
       echo "------------------------------------------------------------------------------"
    fi
done

echo "End Time for $0 "`date` 
echo "============================================================================="
exit ${EXT_STS}
