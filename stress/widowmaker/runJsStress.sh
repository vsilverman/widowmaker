#!/bin/bash 
# Modify QA_HOME with correct path before running this script
export QA_HOME=/space/sundeep/trunk/qa
export LOG_JAR=slf4j-api-1.7.4.jar

a=`ls $QA_HOME/lib/*.jar`

for i in $a; do  JAR=$JAR:$i ; done
echo $JAR

export CLASSPATH=.$QA_HOME/lib/$LOG_JAR:$CLASSPATH:$JAR

cd $QA_HOME/testscripts/regression

java -DQA_HOME=$QA_HOME test.stress.StressTest -s $QA_HOME/stress/widowmaker/jsStressTests.xml

