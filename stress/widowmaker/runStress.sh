#!/bin/bash -xv
# Modify QA_HOME with correct path before running this script
export QA_HOME=/space/builder/trunk/qa

a=`ls $QA_HOME/lib/*.jar`

for i in $a; do  JAR=$JAR:$i ; done
echo $JAR

export CLASSPATH=$CLASSPATH:$JAR

java -DQA_HOME=$QA_HOME test.stress.StressTest -s $QA_HOME/stress/widowmaker/stressTests.xml

