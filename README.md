# Smart Thermostat - AWS - HBase - Spark

This repository accompanies the blog post [Making your own smart 'machine learning' thermostat using Arduino, AWS, HBase, Spark, Raspberry PI and XBee](http://niektemme.com/2015/08/09/smart-thermostat/). This blog post describes building and programming your own smart thermostat. 

This smart thermostat is based on three feedback loops. 
- I. The first loop is based on an Arduino directly controlling the boiler. [Smart Thermostat - Arduino Repository](https://github.com/niektemme/smarttherm-arduino)
- II. The second feedback loop is a Raspberry PI that receives temperature data and boiler status information from the Arduino and sends instructions to the Arduino. [Smart Thermostat - Raspberry PI Repository](https://github.com/niektemme/smarttherm-rpi)
- **II. The third and last feedback loop is a server in the Cloud. This server uses machine learning to optimize the boiler control model that is running on the Raspberry PI. (this repository)**

![Smart thermostat overview - three feedback loops](https://niektemme.files.wordpress.com/2015/07/schema_loop3.png)

## Installation & Setup
The cloud part of the Smart Thermostat is based on one Python script (outside_temperature), two Spark jobs (score & cluster) and one Scala application to initiate the score Spark job. A more detailed description of how these work together is described in the blog post mentioned above. 

### HBase tables
Running HBase and creating the required HBase tables is described in the 'Installation & Setup' paragraph in the readme included in the [Smart Thermostat - Raspberry PI Repository](https://github.com/niektemme/smarttherm-rpi)

### Updating outside temperature
The outtempupdate Python script in the outside_temperature sub folder of this repository can run as a hourly cron or simply be put in the /etc/cron.hourly/ folder. Scripts in a cron.* folder can not have a file extention. Also the script has to be executable. Do: sudo chmod a+x ./outtempupdate from the script location.

#### Updating the city of the outside temperature location
The outtempupdate script gets the temperature information from http://www.openweathermap.org Update the line bellow to the correct url for your location.

weget = urllib2.urlopen("http://api.openweathermap.org/data/2.5/weather?id=2759794")

### Running Apache Spark
The Spark jobs require a running Spark cluster. The [Spark website](https://spark.apache.org/docs/latest/cluster-overview.html) describes how to set this up.

### Compiling scala Spark using Apache Maven
The score, runscore and cluster are written in Scala. The source code is included in the different sub directories of this repository. To compile the source code to .jar files that can be run by the Apache Spark cluster, including the required dependencies [Apache Maven](http://maven.apache.org) can be used. To compile the scala code using Maven run the following command from each of the three sub folders: mvn package

#### .pom files
Apache Maven uses .pom files to include dependencies and configure compiling the source code.
The maven .pom files are included in the different sub folders of this repository. In the .pom files the required dependencies to HBase and Spark are included. In the included .pom files specific versions of HBase are referenced. These need to be changed to the correct versions of HBase running on your cluster.

### Log files
For the score and cluster Spark job the internal logging system of Apache Spark are used.

The runscore scala application uses jog4j By default a log file called scorelog.log is added to the same directory of the runscore.lib when the scala application is first run. Modify the conf/log4j.properties to set the log file and logging configuration.

### HBase .jar dependencies
In order for the Spark jobs and scala application to connect to the HBase server, the required HBase .jar files have to be included in the class path when running the jobs and application. For the cluster Spark job and runscore Spark application the class path is set in cron job that starts the job or application (see the 'Running' paragraph bellow).

The score Spark job is started form the runscore scala application and therefore the HBase dependencies for the score job are currently set in in the runscore source code in the sourceFilesAt() function. 

## Running

### Score
The score Spark job is run from the runscore scala application.

### Runscore
To periodically start the runscore application add the following cron job. This checks every hour if there are uploaded scenarios that need to be scored. Make sure to correctly set the directory of the score .jar file, the HBase-site conf file and log4.properties file.

5 * * * * scala -cp "/usr/local/hbase/lib/*:/usr/local/spark/conf/hbase-site.xml:/usr/local/spark/tempniek/runscore/target/score-1.0-SNAPSHOT.jar" -Dlog4j.configuration=file:////usr/local/spark/tempniek/runscore/conf/log4j.properties runscoretemp

### Cluster
The cluster Spark job is directly initiated from a cron job. To periodically start the runscore application add the following cron job. This checks every hour if there are uploaded scenarios that need to be scored. Make sure to correctly set the HBase .jar dependencies, Spark server ip address, and cluster .jar file location.

10 11 * * * /usr/local/spark/bin/spark-submit --driver-class-path /usr/local/hbase/lib/hbase-server-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-protocol-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-hadoop2-compat-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-client-0.98.7-hadoop2.jar:/usr/local/hbase/lib/hbase-common-0.98.7-hadoop2.jar:/usr/local/hbase/lib/htrace-core-2.04.jar:/usr/local/hbase/lib/guava-12.0.1.jar --deploy-mode "cluster" --class "clustertemp" --master spark://ip-172-31-7-254:6066 /usr/local/spark/tempniek/cluster/target/cluster-1.0-SNAPSHOT.jar


## Acknowledgements
The code used in this project is often based on wonderful and clearly written examples written by other people. I would especially like to thank the following people (alphabetical order).

- Aravindu Sandela - bigdatahandler - http://bigdatahandler.com
- Dave - Desert Home - http://www.desert-home.com
- Lady Ada - Adafruit - http://www.adafruit.com
- Lars George - HBase definitive guide - http://www.larsgeorge.com
- Luckily Seraph Chutium - ABC Networks Blog - http://www.abcn.net
- Michael Bouvy - http://michael.bouvy.net
- Paco Nathan - O'Reilly Media - http://iber118.com/pxn/
- Robert Faludi - Digi International - http://www.faludi.com
- Stephen Phillips - The University of Southampton, IT Innovation Centre  - http://blog.scphillips.com
