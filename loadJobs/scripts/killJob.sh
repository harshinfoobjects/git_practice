#!/bin/bash

##################################################################################################################################################
## Name: killJob.sh
## Purpose: kill specific spark job.
## Author: ksuthar
## Arguments: #1. job name (string)
## Created Date: 2017-12-12
## Modified Date: 
## Modification 
## Modified Date: 
###################################################################################################################################################


echo "Start Time of $0 "`date`

if [ $# -ne 1 ]
then
    echo "[Error_1000 in $0]: Invalid arguments, should be 1 <job_name>"
    echo "Expected format is $0 <job_name>"
    echo -n "$0 finished with error at "
    date
    echo "============================================================================="
    exit 1000
else
    JOB_NAME=$1
    applicationId=$(yarn application -list -appStates RUNNING | awk -v tmpJob=$JOB_NAME '{ if( $2 == tmpJob) print $1 }')
     
    #If job name found, kill the application else report the message
    if [ ! -z $applicationId ]
    then
    yarn application -kill $applicationId
    echo " "
    echo "JOB: ${JOB_NAME}, Killed applicationId : ${applicationId} successfully"
    echo " "
    else
    printf "Job name didn't match. Please check your input job name\n"
    fi
fi
echo "End Time for $0 "`date`
echo "============================================================================="
exit 0
