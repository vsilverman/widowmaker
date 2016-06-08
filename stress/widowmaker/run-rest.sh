BASEDIR=$(dirname $0)
pushd $BASEDIR/../.. > /dev/null 2>&1
LOG4J_FILE=$1
if [ -z "$LOG4J_FILE" ]
then
    LOG4J_FILE=/tmp/wire_trace.out
fi
QA_HOME=`pwd`
popd  > /dev/null 2>&1
LIBS=\
$QA_HOME/stress/widowmaker/:\
$QA_HOME/testscripts/EC2/tools/ec2-api-tools-1.6.7.2/lib/log4j-1.2.14.jar:\
$QA_HOME/lib/client-api-java-3.0-SNAPSHOT.jar:\
$QA_HOME/lib/commons-codec-1.7.jar:\
$QA_HOME/lib/slf4j-api-1.7.4.jar:\
$QA_HOME/lib/commons-logging-1.1.1.jar:\
$QA_HOME/lib/httpclient-4.1.1.jar:\
$QA_HOME/lib/httpcore-4.1.jar:\
$QA_HOME/lib/jersey-apache-client4-1.17.jar:\
$QA_HOME/lib/jersey-client-1.17.jar:\
$QA_HOME/lib/jersey-core-1.17.jar:\
$QA_HOME/lib/jersey-multipart-1.17.jar:\
$QA_HOME/lib/mimepull-1.6.jar:\
$QA_HOME/lib/jbossjta.jar:\
$QA_HOME/lib/jta-1.1.jar:\
$QA_HOME/lib/commons-httpclient-2.0.jar:\
$QA_HOME/lib/jackson-core-2.4.1.jar:\
$QA_HOME/lib/jackson-annotations-2.4.1.jar:\
$QA_HOME/lib/jackson-databind-2.4.1.jar:\
$QA_HOME/lib/xcc.jar
#$QA_HOME/lib/logback-core-1.0.12.jar:\
#$QA_HOME/lib/logback-classic-1.0.12.jar:\
#$QA_HOME/lib/jboss-logging.jar:\
CLASSPATH=$LIBS:$QA_HOME/testscripts/regression
# see stress/widowmaker/log4j.properties for other log4j settings
java -DQA_HOME=$QA_HOME -DRESTCLIENT=javaapi \
     -Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.Log4JLogger \
     -Dwidowmaker.log4j.appender=org.apache.log4j.RollingFileAppender \
     -Dwidowmaker.log4j.File=$LOG4J_FILE \
     -cp $CLASSPATH test.stress.StressTest -s $QA_HOME/stress/widowmaker/restTests.xml
