#!/bin/bash
cd ${TWB_BASE}/config/jobs/twb_c1
kinit svc-ra-bicoeloaddev@RA-INT.COM -k -t svc-ra-bicoeloaddev.keytab
export HADOOP_CONF_DIR=/etc/hadoop/conf:/etc/hive/conf
nohup spark-submit --master yarn-cluster --name c1_md_parser --keytab "svc-ra-bicoeloaddev.keytab" --principal "svc-ra-bicoeloaddev@RA-INT.COM"  --conf "spark.dynamicAllocation.enabled=false" --num-executors 3 --driver-memory 2g --executor-memory 2g --conf "spark.streaming.unpersist=true" --conf "spark.executor.heartbeatInterval=20s" --conf "spark.streaming.kafka.maxRetries=5" --conf "spark.streaming.backpressure.enabled=true" --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -Djava.security.krb5.conf=/etc/krb5.conf" --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -Djava.security.krb5.conf=/etc/krb5.conf" --conf spark.yarn.stagingDir=hdfs:///tmp/spark/ --files "c1_md_parser.properties" --jars "${TWB_BASE}/build/ImpalaJDBC41.jar,${TWB_BASE}/build/ql.jar,${TWB_BASE}/build/TCLIServiceClient.jar" --conf spark.yarn.maxAppAttempts=4 --conf spark.yarn.max.executor.failures=24 --conf spark.task.maxFailures=8 --conf spark.hadoop.fs.hdfs.impl.disable.cache=true --conf "spark.memory.storageFraction=0.3" ${TWB_BASE}/build/xml-parser-spark-service-0.1.0-SNAPSHOT.jar c1_md_parser.properties > ${TWB_BASE}/loadJobs/logs/twb_c1/c1_md_parser.log 2>&1 & echo $! > c1_md_parser.pid
