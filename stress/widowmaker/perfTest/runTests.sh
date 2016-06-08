#!/bin/bash -xv
CURRENT=`pwd`
cd ../../..
QA_HOME=`pwd`
cd -
export QA_HOME
export CLASSPATH=

cp multi-statement-diffdelta.xml $QA_HOME/scripts/tests/


a=`ls $QA_HOME/lib/*.jar`

for i in $a; do  JAR=$JAR:$i ; done
echo $JAR
cd $QA_HOME/testscripts/regression

export CON_COUNT=`netstat -anp | grep WAIT | grep 5275 | wc -l`

while [ $CON_COUNT -ge 100 ] 
do
    $CON_COUNT=`netstat -anp | grep WAIT | grep 5275 | wc -l`
    echo "number of connections" 
    echo $CON_COUNT 
    sleep 10
done

java -classpath .$JAR -DQA_HOME=$QA_HOME test.stress.StressTest -t $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1.xml -c  $QA_HOME/stress/widowmaker/perfTest/connectInfo.xml

cd $QA_HOME

while [ $CON_COUNT -ge 100 ]
do 
    $CON_COUNT=`netstat -anp | grep WAIT | grep 5275 | wc -l`
    echo "number of connections" 
    echo $CON_COUNT 
    sleep 10
done

make ut tname=batch1.xml

sudo /usr/local/sbin/mladmin restart

sleep 30


cd $QA_HOME/testscripts/regression

java -classpath .$JAR -DQA_HOME=$QA_HOME test.stress.StressTest -t $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch10.xml -c  $QA_HOME/stress/widowmaker/perfTest/connectInfo.xml

cd $QA_HOME

while [ $CON_COUNT -ge 100 ]
do
    $CON_COUNT=`netstat -anp | grep WAIT | grep 5275 | wc -l`
    echo "number of connections" 
    echo $CON_COUNT 
    sleep 10
done

make ut tname=batch10.xml

sleep 30

sudo /usr/local/sbin/mladmin restart

sleep 30


cd $QA_HOME/testscripts/regression

java -classpath .$JAR -DQA_HOME=$QA_HOME test.stress.StressTest -t $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch100.xml -c  $QA_HOME/stress/widowmaker/perfTest/connectInfo.xml

cd $QA_HOME

while [ $CON_COUNT -ge 100 ]
do
    $CON_COUNT=`netstat -anp | grep WAIT | grep 5275 | wc -l`
    echo "number of connections" 
    echo $CON_COUNT 
    sleep 10
done

make ut tname=batch100.xml

sleep 30 

sudo /usr/local/sbin/mladmin restart

sleep 30 

cd $QA_HOME/testscripts/regression

java -classpath .$JAR -DQA_HOME=$QA_HOME test.stress.StressTest -t $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch500.xml -c  $QA_HOME/stress/widowmaker/perfTest/connectInfo.xml

cd $QA_HOME

while [ $CON_COUNT -ge 100 ]
do 
    $CON_COUNT=`netstat -anp | grep WAIT | grep 5275 | wc -l`
    echo "number of connections" 
    echo $CON_COUNT 
    sleep 10
done

make ut tname=batch500.xml

sleep 30

sudo /usr/local/sbin/mladmin restart

sleep 30


cd $QA_HOME/testscripts/regression

java -classpath .$JAR -DQA_HOME=$QA_HOME test.stress.StressTest -t $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml $QA_HOME/stress/widowmaker/perfTest/load/load-batch1000.xml -c  $QA_HOME/stress/widowmaker/perfTest/connectInfo.xml

cd $QA_HOME

make ut tname=batch1000.xml

sleep 30

sudo /usr/local/sbin/mladmin restart

sleep 30

make ut tname=multi-statement-diffdelta.xml BASELINE=$1

