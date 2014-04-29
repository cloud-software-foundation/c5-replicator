c5
================

C5 is a HBase API compatible database. C5 is a simple, scalable open source 
distributed database. It is an ACID database, of the flavour of Big Table. 
It supports three types of compatibility.  HBase API 
compatability(HTable/HBaseAdmin), HFile compatibility, and HBase replication. 

This project has 2 major modules. C5DB (The Server) and C5DB-CLIENT (The Client). To build this project users can simply mvn install (With optional -DskipTests) to build the c5 daemons, client libraries and optionally run all of our tests.

Users can start the c5server in single node mode, by simply changing into the 
c5db directory and running  ./bin/run_test_server.sh, after they have built 
the project. One can think of run_test_server as an example of how to start 
the server.

-D options of c5db include

regionServerPort=<port#>
webServerPort=<port#>
cluserName=<The name of the cluster>
-Dorg.slf4j.simpleLogger.defaultLogLevel=<log level>
-Dorg.slf4j.simpleLogger.log.org.eclipse.jetty=<log level>

EXAMPLES
Examples of how to access the server can be found in c5-client-tests. In 
adddition, a web status console will start on port 31337. Information about
the cluster can be found there.


TROUBLESHOOTING
On Mac OSX:
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
