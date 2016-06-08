#!/bin/bash 
#
# Make sure to update connection information in configuration xml before running this script
# For example : You may need to update stress/widowmaker/connectInfo.xml 
#
#
#  Copyright 2003-2013 MarkLogic Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License"); 
#  you may not use this file except in compliance with the License. 
#  You may obtain a copy of the License at 
#
#     http://www.apache.org/licenses/LICENSE-2.0 
#
#  Unless required by applicable law or agreed to in writing, software 
#  distributed under the License is distributed on an "AS IS" BASIS, 
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
#  See the License for the specific language governing permissions and 
#  limitations under the License.
#


. ./inc/allmakes

if [ -z $TOP ]
then
    echo -e "\nPlease run make before using this script"
    exit -1
fi

QA_HOME=$TOP

echo "QA_HOME=$QA_HOME"

a=`ls $QA_HOME/lib/*/*.jar`

for i in $a; do  JAR=$JAR:$i ; done
echo $JAR

export CLASSPATH=.:$CLASSPATH:$JAR

cd $QA_HOME/src

java -DQA_HOME=$QA_HOME test.stress.StressTest -s $QA_HOME/stress/widowmaker/sampleTest.xml

