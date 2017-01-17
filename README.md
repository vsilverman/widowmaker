# widowmaker
Stress test engine for MarkLogic projects

1. INTRODUCTION
------------
The goal of this project is to allow users to be able to run stress tests against a MarkLogic server instance. 

You should be able to adjust the config files to suit your environment and kick off the tests.


2. REQUIREMENTS
------------
This project requires you to have the following installed on your machine

2.1 Operating System : 
 `Linux, Mac, Windows (Look at product documentation for specific versions required)`

2.2 Software : 
```
 Java 1.8 (or higher)
 GNU make (or equivalent)
 MarkLogic Server (Version 8 or higher) - visit http://developer.marklogic.com/ for a free download
```

3. DEPENDENCIES
----------------
All the dependent jar files are included in the lib directory

Other than the above, there are no known dependency files.


4. STEPS
--------
```
4.1. Make sure MarkLogic Server is installed and is running.
   For instructions, please refer to http://docs.marklogic.com/guide/installation 

4.2. Make sure you have created the following objects in MarkLogic:
     - Database
     - Forests (created and attached to database)
     - Xdbc server pointing to the above database
     - A user with admin role

4.3. Build the code by running 'make' from top level directory.

 
4.4. Tune the configuration files under 'stress/widowmaker'
   For more details, see the relevant section below.


4.5. Goto 'src' directory and run : java -DQA_HOME=<path-to-top> test.stress.StressTest -s <path-to-top>/stress/widowmaker/stressTests.xml
   where '<path-to-top>' is the absolute path to the top level dorectory where this project is downloaded to.
  
    The following example script can be reused to suit your needs :  runSampleTest.sh  
```


5. TUNING CONFIGURATION FILES
------------------------------

All the configuration files are in xml format which are under <path-to-top>/stress/widowmaker

There are three different kinds of configuration files.

```
5.1. Stress Test configuration file (the HOW)

  example config file: <path-to-top>/stress/widowmaker/stressTests.xml

  <stress-config>
        <connect-location>QA_HOME/stress/widowmaker/connectInfo.xml</connect-location>
        <min-users>5</min-users>
        <max-users>25</max-users>
        <stress-tests>
                <test-location>QA_HOME/stress/widowmaker/loadtests/loadTester1.xml</test-location>
                <test-location>QA_HOME/stress/widowmaker/crudtests/deletetest1.xml</test-location>
        </stress-tests>
</stress-config>
   
 This configuration file has key information on how to run your tests including where to get the connection information from, howmany threads to run, which tests to include. This file needs to be passed for starting the stress test as you will see in the STEPS section.

 5.1.a. <connect-location>QA_HOME/stress/widowmaker/connectInfo.xml</connect-location>
    This is path to a configuration file where connection information exists.

 5.1.b. <min-users>5</min-users>
    Minimum number of threads to maintain at all times. 

 5.1.c. <max-users>25</max-users>
    Maximum number of threads that can be spawned. The system will fire up these many number of threads and wait. As the threads finish up and if the number of threads reach configured 'min-users', more threads will be spawned again until thread count reaches configured 'max-users' and this process continues. You may tune it just like you would for any multi-threaded application, depending on the system resources you have.

 5.1.d. <stress-tests>
                <test-location>QA_HOME/stress/widowmaker/loadtests/loadTester1.xml</test-location>
                <test-location>QA_HOME/stress/widowmaker/crudtests/deletetest1.xml</test-location>
    </stress-tests>
    Pointing to the XML files where test configurations are written.


5.2. Connection Information (the WHERE)

 example config file: <path-to-top>/stress/widowmaker/connectInfo.xml

 This configuration file has connection information on where the tests should point to.

  <server type="master">
                <port>5275</port>
                <username>admin</username>
                <password>admin</password>
                <host>localhost</host>
                <database>MedlineDB</database>
  </server>
  
 5.2.a. <server type="master">
    Enclose each server configuration in each server configuration element. The attribute 'type' is to specify whether the server (cluster) is master or replica

 5.2.b. <port>5275</port>
    port to connect to.

 5.2.c. <username>admin</username>
    username to use while connecting.

 5.2.d. <password>admin</password>
    password to use.

 5.2.e. <host>localhost</host>
    Host to connect to.

 5.2.f. <database>MedlineDB</database>
    Database to point to. Use this only with REST related tests. For other tests, point your appserver at configured port to the desired database.

    
5.3. Test configuration (the WHAT)

 example configuration file : <path-to-top>/stress/widowmaker/loadtests/loadTester.xml

 <stresstest>
        <testtype>loadtester</testtype>
        <numloops>1</numloops>
        <toscreen>true</toscreen>
        <sleeptime>1</sleeptime>
        <logoption>debug</logoption>
        <logfilename>auto</logfilename>
        <outputfile>auto</outputfile>
        <operations>
                <create>10000</create>
                <checkinterval>100</checkinterval>
                <loaddir>QA_HOME/testdata/foodir</loaddir> 
                <generatequery numgenerated=100>attr numgenerated denotes how many per exe</generatequery>
                <createmodule>QA_HOME/app/moduletouse.xqy</createmodule> 
                <autogenerate>false</autogenerate>
                <multistatement batchsize=10 type=commit/>
        </operations>
 </stresstest>

 5.3.a. <stresstest>
    Enclose all configuration in this element.

 5.3.b. <testtype>loadTester</testtype>
    This will directly map to a java class defined. Following are the current mappings:
      loadTester = XccLoadTester
      restLoadTester = RestLoadTester
      bulkLoadTester = BulkLoadTester
      crudTester = CRUDTester
      restCrudTester = RestCRUDTester
      hashLockTester = HashLockTester
      queryTester = QueryTester
      restQueryTester = RestQueryTester
      sqlTester = SQLTester

    If you plan to write new classes, this is how you can call them in your test.

 5.3.c. <numloops>1</numloops>
        Number of iterations to run the test in each thread

 5.3.d. <toscreen>true</toscreen>
        Whether or not to emit all the logging to stdout. (Not used right now)

 5.3.e. <sleeptime>1</sleeptime>
        Sleep interval in milliseconds, between each iteration (loop).

 5.3.f. <logoption>debug</logoption>
         How fine grained you would like the logs to be. (Not used right now)

 5.3.g. <logfilename>auto</logfilename>
         Provide a filename for logging. 'auto' will autogenerate logfile  (Not used right now)

 5.3.h.  <outputfile>auto</outputfile>
         Output file. (Not used right now)

 5.3.i. <operations>
         All the operations you would like to do in each loop (Not used right now)

 5.3.j. <create>10000</create>
        Number of items to create
      
 5.3.k.  <checkinterval>100</checkinterval>
        Wait for these many documents to load before verification
      
 5.3.l.  <loaddir>QA_HOME/testdata/foodir</loaddir>
        Path to all the test data to be loaded into the database.

 5.3.m.  <generatequery numgenerated=100>attr numgenerated denotes how many per exe</generatequery>
        To generate a query based on key words (logic is in your class)
     
 5.3.n.  <createmodule>QA_HOME/app/moduletouse.xqy</createmodule>
         Module to use for generating data
     
 5.3.o.  <autogenerate>false</autogenerate>
         Auto generate testdata. 

 5.3.p.  <multistatement batchsize=10 type=commit/>
         USe this for multi-statement transactions. 'type' can be 'commit' or 'rollback'.
```


6. MAINTAINERS
-----------
Current maintainers:
```
 * Larry Ratcliff, MarkLogic Corporation
 * Sundeep Vempati, MarkLogic Corporation
```


